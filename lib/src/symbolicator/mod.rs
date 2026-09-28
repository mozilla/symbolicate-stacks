use super::Config;
use crate::config::SymbolServers;
use anyhow::Context;
use debugid::DebugId;
use download_cache::fs::FsCache;
use futures_util::{future::FutureExt, stream::StreamExt};
use samply_symbols::{
    FileAndPathHelper, FileAndPathHelperResult, FileLocation, FramesLookupResult, LibraryInfo,
    LookupAddress, SymbolManager, SyncAddressInfo,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;

mod breakpad;
mod symbol_maps;
mod symsrv;

// Limits to the top and bottom frames, relative to a traceback ("top" being the innermost frame).
// The rest of the frames are omitted. MAX_TOP_FRAMES should be at least 10 for siggen to work
// properly.
const MAX_TOP_FRAMES: usize = 50;
const MAX_BOTTOM_FRAMES: usize = 50;
const MAX_FRAMES: usize = MAX_TOP_FRAMES + MAX_BOTTOM_FRAMES;

pub struct Symbolicator {
    symbol_manager: Arc<SymbolManager<FileHelper>>,
    symbol_maps: symbol_maps::SymbolMaps,
    only_crashing_thread: bool,
}

impl Symbolicator {
    pub fn new(
        config: &Config,
        _status: &Arc<crate::Status>,
        only_crashing_thread: bool,
    ) -> anyhow::Result<Self> {
        let helper = FileHelper::new(config)?;
        Ok(Symbolicator {
            symbol_manager: Arc::new(SymbolManager::with_helper(helper)),
            symbol_maps: Default::default(),
            only_crashing_thread,
        })
    }

    pub fn symbolicate(
        &self,
        stack_traces_json: String,
        _channel: &str,
    ) -> impl std::future::Future<Output = anyhow::Result<Symbolicated>> {
        let symbol_map_getter = self.symbol_maps.getter(self.symbol_manager.clone());

        // Spawn a task to ensure that all calls to symbolicate are executed concurrently, which
        // avoids potential deadlock if the cache limit is reached (sequencing would introduce
        // unnecessary execution dependencies).
        let only_crashing_thread = self.only_crashing_thread;
        tokio::spawn(async move {
            let mut stack_traces =
                match serde_json::from_str::<json::StackTraces>(&stack_traces_json) {
                    Ok(st) => st,
                    Err(e) => {
                        log::error!("failed to parse stack traces: {e:#}");
                        Default::default()
                    }
                };

            // Temporary workaround for https://bugzilla.mozilla.org/show_bug.cgi?id=1954819
            // Firefox only configures minidump-analyzer to include all threads for crashes
            // identified as hangs. They are far less common than normal crashes, for which only
            // the crashing thread is included (and the crash_thread index is 0). Default to 0 here
            // to be able to process the old data with potentially missing `crash_thread` values.
            if stack_traces.crash_thread.is_none() {
                stack_traces.crash_thread = Some(0);
            }

            fn thread_frames(t: json::Thread<'_>) -> Vec<Frame> {
                t.frames.into_iter().map(Frame::from).collect()
            }

            let mut threads = {
                let mut threads = stack_traces
                    .threads
                    .into_iter()
                    .map(Memo::new)
                    .collect::<Vec<_>>();

                if let Some(mut ind) = stack_traces.crash_thread {
                    // Add an empty thread and adjust the crash thread if out-of-bounds
                    if ind >= threads.len() {
                        log::debug!("crashing thread index out of bounds: creating a new thread with no frames as the crashing thread");
                        ind = threads.len();
                        stack_traces.crash_thread = Some(ind);
                        threads.push(Memo::output(vec![]));
                    }

                    // Use the crash address directly if no frame data is available
                    let frames = threads[ind].get(thread_frames);
                    if frames.is_empty() {
                        if let Some(ip) = stack_traces
                            .crash_address
                            .as_ref()
                            .and_then(|s| parse_hex(s).ok())
                        {
                            log::debug!("no frames in crashing thread: using crash address");
                            frames.push(Frame {
                                ip,
                                module_index: None,
                                info: Default::default(),
                            });
                        }
                    }
                }

                if only_crashing_thread {
                    if let Some(ind) = stack_traces.crash_thread {
                        threads
                            .into_iter()
                            .nth(ind)
                            .map(|m| m.into_output(thread_frames))
                            .into_iter()
                            .collect::<Vec<_>>()
                    } else {
                        Default::default()
                    }
                } else {
                    threads
                        .into_iter()
                        .map(|m| m.into_output(thread_frames))
                        .collect()
                }
            };

            // Limit thread frames to MAX_FRAMES.
            for t in &mut threads {
                if t.len() > MAX_FRAMES {
                    let to_omit = t.len() - MAX_FRAMES;
                    t.splice(
                        MAX_TOP_FRAMES..(MAX_TOP_FRAMES + to_omit),
                        [Frame {
                            ip: 0,
                            module_index: None,
                            info: vec![FrameInfo::Omitted { omitted: to_omit }],
                        }],
                    );
                }
            }

            let used_modules: std::collections::HashSet<usize> = threads
                .iter()
                .flat_map(|t| t)
                .filter_map(|f| f.module_index)
                .collect();

            let modules: Vec<(usize, Module)> = used_modules
                .into_iter()
                .filter_map(|i| match Module::try_from(stack_traces.modules.get(i)?) {
                    Err(e) => {
                        log::debug!("failed to get module: {e:#}");
                        None
                    }
                    Ok(module) => Some((i, module)),
                })
                .collect();

            // This creates a FuturesUnordered for resolving modules. As they are available, we'll
            // fill out all the relevant frames. It's important to poll them all concurrently to
            // avoid dependency deadlock (accidentally imposing some ordering of module
            // availability).
            let mut modules = symbol_map_getter.get_symbol_maps(
                modules
                    .into_iter()
                    .map(|(index, module)| {
                        let key = Key {
                            debug_name: module.debug_file.clone(),
                            debug_id: module.debug_id.clone(),
                        };
                        ((index, module), key)
                    })
                    .collect(),
            );

            'modules: while let Some(((index, module), map)) = modules.next().await {
                let map = match map {
                    Ok(Some(map)) => map,
                    Ok(None) => continue 'modules,
                    Err(e) => {
                        let mut eref: Option<&(dyn std::error::Error + 'static)> = Some(e.as_ref());
                        while let Some(e) = eref {
                            if let Some(e) = e.downcast_ref::<NotFoundError>() {
                                log::debug!("{e}");
                                continue 'modules;
                            }
                            eref = e.source();
                        }
                        return Err(e.into());
                    }
                };
                for ping_frame in threads
                    .iter_mut()
                    .flat_map(|t| t)
                    .filter(|f| f.ip != 0 && f.module_index == Some(index))
                {
                    let offset: u32 = match (ping_frame.ip - module.base_address).try_into() {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!(
                                "error getting module offset (ip={}, base_address={}): {e}",
                                ping_frame.ip,
                                module.base_address
                            );
                            continue;
                        }
                    };

                    if let Some(SyncAddressInfo { frames, symbol }) =
                        map.lookup_sync(LookupAddress::Relative(offset))
                    {
                        let create_frame_info = |f: Option<samply_symbols::FrameDebugInfo>| {
                            let (function, file_path, line) = f
                                .map(|f| (f.function, f.file_path, f.line_number))
                                .unwrap_or_default();
                            FrameInfo::Symbol {
                                offset: Some(format!("{:#018x}", ping_frame.ip)),
                                module: module
                                    .filename
                                    .as_ref()
                                    .or(Some(&module.debug_file))
                                    .cloned(),
                                module_offset: Some(format!("{offset:#018x}")),
                                function: function.or(Some(symbol.name.clone())),
                                function_offset: Some(format!("{:#018x}", offset - symbol.address)),
                                file: file_path.map(|p| match p.mapped_path() {
                                    Some(mapped) => mapped.to_special_path_str(),
                                    None => p.raw_path().to_string(),
                                }),
                                line,
                            }
                        };

                        if let Some(FramesLookupResult::Available(frames)) = frames {
                            ping_frame.info = frames
                                .into_iter()
                                .map(|f| create_frame_info(Some(f)))
                                .collect();
                        } else {
                            ping_frame.info = vec![create_frame_info(None)];
                        }
                    }
                }
            }

            // Combine symbolicated thread frames.
            let threads = threads
                .into_iter()
                .map(|t| {
                    let mut frames: Vec<FrameInfo> = t
                        .into_iter()
                        .flat_map(|f| f.into_frame_info(&stack_traces.modules))
                        .collect();

                    // Limit combined frames to MAX_FRAMES again. Symbolication may have introduced
                    // more inlined frames after the first pass. We do the first pass as an optimization to
                    // avoid unnecessary frame lookups.
                    if frames.len() > MAX_FRAMES {
                        let to_omit = frames.len() - MAX_FRAMES;
                        // If there is already an `omitted` entry, it is guaranteed to be in the
                        // discarded range, because `into_frame_info` always produces at least one
                        // value and we are using the same top/bottom limits (i.e., the original
                        // `MAX_TOP_FRAMES` will expand to at least `MAX_TOP_FRAMES` here which
                        // will not have an omitted value, and likewise for the
                        // `MAX_BOTTOM_FRAMES`.
                        let total_omitted: usize = frames
                            [MAX_TOP_FRAMES..(MAX_TOP_FRAMES + to_omit)]
                            .iter()
                            .map(|info| match info {
                                FrameInfo::Omitted { omitted } => *omitted,
                                _ => 1,
                            })
                            .sum();
                        frames.splice(
                            MAX_TOP_FRAMES..(MAX_TOP_FRAMES + to_omit),
                            [FrameInfo::Omitted {
                                omitted: total_omitted,
                            }],
                        );
                    }

                    ThreadInfo { frames }
                })
                .collect();

            Ok(Symbolicated {
                reason: stack_traces.crash_type.map(|s| s.into_owned()),
                crashing_thread: only_crashing_thread.then_some(0).or(stack_traces.crash_thread),
                threads,
            })
        })
        .map(|r| r.map_err(anyhow::Error::from).and_then(|r| r))
    }
}

enum Memo<T, U> {
    Input(Option<T>),
    Output(U),
}

impl<T, U> Memo<T, U> {
    pub fn new(input: T) -> Self {
        Memo::Input(Some(input))
    }

    pub fn output(output: U) -> Self {
        Memo::Output(output)
    }

    pub fn get<F: FnOnce(T) -> U>(&mut self, f: F) -> &mut U {
        if let Self::Input(i) = self {
            *self = Self::Output(f(i.take().unwrap()));
        }
        if let Self::Output(o) = self {
            o
        } else {
            unreachable!()
        }
    }

    pub fn into_output<F: FnOnce(T) -> U>(self, f: F) -> U {
        match self {
            Self::Input(i) => f(i.unwrap()),
            Self::Output(o) => o,
        }
    }
}

struct Module {
    base_address: usize,
    filename: Option<String>,
    debug_file: String,
    debug_id: DebugId,
}

#[derive(Debug)]
struct Frame {
    ip: usize,
    module_index: Option<usize>,
    info: Vec<FrameInfo>,
}

impl Frame {
    /// Return the frame info, or create a frame info with the minimal information available if no
    /// info was populated.
    ///
    /// This is guaranteed to return a non-empty Vec.
    pub fn into_frame_info(self, modules: &[json::Module]) -> Vec<FrameInfo> {
        if self.info.is_empty() {
            let mut module = None;
            let mut module_offset = None;
            if let Some(m) = self.module_index.and_then(|i| modules.get(i)) {
                module = m
                    .filename
                    .as_ref()
                    .or(m.debug_file.as_ref())
                    .map(|s| s.to_string());
                if let Some(base) = m.base_address.as_ref().and_then(|b| parse_hex(&b).ok()) {
                    module_offset = Some(format!("{:#018x}", self.ip - base));
                }
            }
            vec![FrameInfo::Symbol {
                offset: Some(format!("{:#018x}", self.ip)),
                module,
                module_offset,
                function: None,
                function_offset: None,
                file: None,
                line: None,
            }]
        } else {
            self.info
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Symbolicated {
    pub reason: Option<String>,
    pub crashing_thread: Option<usize>,
    pub threads: Vec<ThreadInfo>,
}

#[derive(Debug, serde::Serialize)]
pub struct ThreadInfo {
    pub frames: Vec<FrameInfo>,
}

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
pub enum FrameInfo {
    Omitted {
        omitted: usize,
    },
    Error {
        error: String,
    },
    Symbol {
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        module: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        module_offset: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        function: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        function_offset: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        file: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        line: Option<u32>,
    },
}

fn parse_hex(s: &str) -> anyhow::Result<usize> {
    usize::from_str_radix(s.trim_start_matches("0x"), 16)
        .with_context(|| format!("parsing {s} as hex"))
}

impl From<json::Frame<'_>> for Frame {
    fn from(value: json::Frame) -> Self {
        let mut ret = Frame {
            ip: 0,
            module_index: value.module_index,
            info: Default::default(),
        };
        match parse_hex(&value.ip) {
            Ok(ip) => ret.ip = ip,
            Err(e) => {
                log::error!("{e:#}");
                ret.info.push(FrameInfo::Error {
                    error: format!("{e}"),
                });
            }
        }
        ret
    }
}

impl TryFrom<&json::Module<'_>> for Module {
    type Error = anyhow::Error;

    fn try_from(value: &json::Module<'_>) -> Result<Self, Self::Error> {
        Ok(Module {
            base_address: parse_hex(
                value
                    .base_address
                    .as_ref()
                    .context("missing base_address")?,
            )?,
            filename: value.filename.as_ref().map(|s| s.to_string()),
            // Fix the debug file entries of modules to only be the file basenames. This is fixed
            // by bug 1931237 but it will take a while to get into release.
            // `unwrap` will never fail because `rsplit` is guaranteed to return at least one
            // value.
            debug_file: value
                .debug_file
                .as_ref()
                .and_then(|s| (!s.is_empty()).then_some(s))
                .context("missing debug_file")?
                .rsplit("/")
                .next()
                .unwrap()
                .into(),
            debug_id: value
                .debug_id
                .as_ref()
                .and_then(|s| (!s.is_empty()).then_some(s))
                .context("missing debug_id")?
                .parse()
                .with_context(|| format!("'{}'", value.debug_id.as_ref().unwrap()))?,
        })
    }
}

#[derive(Clone)]
struct Location {
    key: Key,
    symindex: bool,
}

impl std::fmt::Debug for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("key", &self.key)
            .field("symindex", &self.symindex)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)?;
        if self.symindex {
            write!(f, ".symindex")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Key {
    pub debug_name: String,
    pub debug_id: DebugId,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.debug_name, self.debug_id.breakpad())
    }
}

impl download_cache::fs::CacheKey for Key {
    fn cache_key(&self) -> std::path::PathBuf {
        let mut path = std::path::PathBuf::new();
        path.push(&self.debug_name);
        path.push(self.debug_id.breakpad().to_string());
        path
    }
}

impl FileLocation for Location {
    fn location_for_dyld_subcache(&self, _suffix: &str) -> Option<Self> {
        None
    }

    fn location_for_external_object_file(&self, _object_file: &str) -> Option<Self> {
        None
    }

    fn location_for_pdb_from_binary(&self, _pdb_path_in_binary: &str) -> Option<Self> {
        None
    }

    fn location_for_source_file(&self, _source_file_path: &str) -> Option<Self> {
        None
    }

    fn location_for_breakpad_symindex(&self) -> Option<Self> {
        let mut loc = self.clone();
        loc.symindex = true;
        Some(loc)
    }

    fn location_for_dwo(&self, _comp_dir: &str, _path: &str) -> Option<Self> {
        None
    }

    fn location_for_dwp(&self) -> Option<Self> {
        None
    }
}

struct FileHelper {
    cache: FsCache,
    symbol_servers: SymbolServers,
    downloads: Semaphore,
    loaded_files: std::sync::Mutex<HashMap<Key, Loaded>>,
}

struct Loaded {
    mapped: Arc<memmap2::Mmap>,
    breakpad_symindex_mapped: BreakpadSymindex,
}

enum BreakpadSymindex {
    None,
    Path(std::path::PathBuf),
    Loaded(Arc<memmap2::Mmap>),
}

impl FileHelper {
    fn new(config: &Config) -> anyhow::Result<Self> {
        let cache_dir = config.cache.directory.clone();
        let symbol_servers = config.symbol_servers.clone();
        let downloads = Semaphore::new(config.symbol_servers.concurrency);
        let client = reqwest::Client::builder()
            .user_agent(super::APP_USER_AGENT)
            .build()?;
        let cache = FsCache::new(cache_dir, client);

        Ok(FileHelper {
            cache,
            symbol_servers,
            downloads,
            loaded_files: Default::default(),
        })
    }

    /*
    fn evict(&self, key: &Key) {
        let entry = self.loaded_files.lock().unwrap().remove(key);
        if let Some(loaded) = entry {
            drop(loaded);
            let paths = [
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_relative_path()),
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_symindex_relative_path()),
                self.cache_dir
                    .join("windows")
                    .join(key.symsrv_relative_path()),
            ];
            for path in paths {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    */

    /*
    fn get_mapped(&self, key: &Key) -> Option<Arc<memmap2::Mmap>> {
        let mut guard = self.loaded_files.lock().unwrap();
        let loaded = guard.get_mut(key)?;
        if loaded.mapped.is_none() {
            let f = std::fs::File::open(
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_relative_path()),
            )
            .ok()
            .or_else(|| {
                std::fs::File::open(
                    self.cache_dir
                        .join("windows")
                        .join(key.symsrv_relative_path()),
                )
                .ok()
            })?;
            loaded.mapped = Some(Arc::new(unsafe { memmap2::Mmap::map(&f) }.ok()?));
        }
        loaded.mapped.clone()
    }
    */

    fn get_breakpad_symindex_mapped(&self, key: &Key) -> Option<Arc<memmap2::Mmap>> {
        let mut guard = self.loaded_files.lock().unwrap();
        let loaded = guard.get_mut(key)?;
        match std::mem::replace(&mut loaded.breakpad_symindex_mapped, BreakpadSymindex::None) {
            BreakpadSymindex::None => None,
            BreakpadSymindex::Path(p) => {
                let f = std::fs::File::open(p).ok()?;
                let mmap = Arc::new(unsafe { memmap2::Mmap::map(&f) }.ok()?);
                loaded.breakpad_symindex_mapped = BreakpadSymindex::Loaded(mmap.clone());
                Some(mmap)
            }
            BreakpadSymindex::Loaded(l) => Some(l.clone()),
        }
    }

    fn symbol_server_urls(&self, key: &Key) -> Vec<download_cache::fs::Url> {
        let mut urls = Vec::new();
        breakpad::get_urls(&self.symbol_servers.breakpad, key, &mut urls);

        if let Err(e) = symsrv::get_urls(&self.symbol_servers.windows, key, &mut urls) {
            log::debug!("failed to get symsrv urls for {key}: {e:#}");
        }

        urls
    }

    async fn do_load_file(&self, location: Location) -> FileAndPathHelperResult<Contents> {
        if !self
            .loaded_files
            .lock()
            .unwrap()
            .contains_key(&location.key)
        {
            let loaded = loop {
                let files = 'files: {
                    // Try an offline get before acquiring a download permit.
                    if let Some(files) = self.cache.get_offline(&location.key) {
                        break 'files files;
                    }

                    let _permit = self.downloads.acquire().await;

                    if let Some(files) = self
                        .cache
                        .get(&location.key, self.symbol_server_urls(&location.key))
                        .await?
                    {
                        break 'files files;
                    } else {
                        return Err(NotFoundError(location).into());
                    }
                };

                match std::fs::File::open(files.primary()) {
                    Ok(file) => {
                        let mapped = Arc::new(
                            unsafe { memmap2::Mmap::map(&file) }.context("failed to map file")?,
                        );
                        break Loaded {
                            mapped,
                            breakpad_symindex_mapped: BreakpadSymindex::Path(
                                breakpad::symindex_path(&files),
                            ),
                        };
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            return Err(e.into());
                        }
                        // Otherwise, download again in case the file was deleted before we
                        // could open it.
                    }
                }
            };

            /*
            let mut errors = AggregateError::new();

            match self.load_breakpad_file(&location.key).await {
                Ok(Some(loaded)) => break 'result loaded,
                Ok(None) => (),
                Err(e) => errors
                    .push(e.context(format!("failed to get breakpad file for {location}"))),
            }

            match self.load_symsrv_file(&location.key).await {
                Ok(Some(loaded)) => break 'result loaded,
                Ok(None) => (),
                Err(e) => errors
                    .push(e.context(format!("failed to get symsrv file for {location}"))),
            }

            // Important: `into_result` will not create an `anyhow::Error` out of the
            // `NotFoundError` here, as there's a bug where an `anyhow::Error` converted to
            // a `Box<std::error::Error>` can't be downcast (see
            // https://github.com/dtolnay/anyhow/issues/379). We check for `NotFoundError`
            // by downcasting, and this function has to return a `Box<std::error::Error>`,
            // so this is a bit fragile.
            return errors.into_result(Err(NotFoundError(location).into()));
            */

            let prev = self
                .loaded_files
                .lock()
                .unwrap()
                .insert(location.key.clone(), loaded);
            assert!(
                prev.is_none(),
                "load_file should only be called with a particular location once"
            );
        }

        if !location.symindex {
            Ok(Contents(
                self.loaded_files
                    .lock()
                    .unwrap()
                    .get(&location.key)
                    .unwrap()
                    .mapped
                    .clone(),
            ))
        } else {
            match self.get_breakpad_symindex_mapped(&location.key) {
                Some(mapped) => Ok(Contents(mapped)),
                None => Err(format!("failed to map symindex file for {location}").into()),
            }
        }
    }
}

struct Contents(Arc<memmap2::Mmap>);

impl std::ops::Deref for Contents {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An error which occurs when a debug file isn't found.
#[derive(Debug)]
struct NotFoundError(Location);

impl std::fmt::Display for NotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not find debug file in symbol servers for {}",
            self.0
        )
    }
}

impl std::error::Error for NotFoundError {}

impl FileAndPathHelper for FileHelper {
    type F = Contents;
    type FL = Location;

    fn get_candidate_paths_for_debug_file(
        &self,
        _info: &LibraryInfo,
    ) -> FileAndPathHelperResult<Vec<samply_symbols::CandidatePathInfo<Self::FL>>> {
        Ok(vec![])
    }

    fn get_candidate_paths_for_binary(
        &self,
        _info: &LibraryInfo,
    ) -> FileAndPathHelperResult<Vec<samply_symbols::CandidatePathInfo<Self::FL>>> {
        Ok(vec![])
    }

    fn get_dyld_shared_cache_paths(
        &self,
        _arch: Option<&str>,
    ) -> FileAndPathHelperResult<Vec<Self::FL>> {
        Ok(vec![])
    }

    fn load_file(
        &self,
        location: Self::FL,
    ) -> std::pin::Pin<
        Box<
            dyn samply_symbols::OptionallySendFuture<Output = FileAndPathHelperResult<Self::F>>
                + '_,
        >,
    > {
        Box::pin(async move { self.do_load_file(location).await })
    }
}

mod json {
    use serde::Deserialize;
    use std::borrow::Cow;

    // Android stack traces are camelCase rather than snake_case (bug 1931891 should fix this), so
    // we use aliases where necessary.

    #[derive(Debug, Default, Deserialize)]
    pub struct StackTraces<'a> {
        #[serde(alias = "crashAddress", default)]
        pub crash_address: Option<String>,
        #[serde(alias = "crashThread", default)]
        pub crash_thread: Option<usize>,
        #[serde(alias = "crashType", default)]
        pub crash_type: Option<Cow<'a, str>>,
        #[serde(borrow, default)]
        pub modules: Vec<Module<'a>>,
        #[serde(default)]
        pub threads: Vec<Thread<'a>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct Module<'a> {
        #[serde(alias = "baseAddress", default)]
        pub base_address: Option<Cow<'a, str>>,
        #[serde(alias = "endAddress", default)]
        #[allow(unused)]
        pub end_address: Option<Cow<'a, str>>,
        #[serde(default)]
        pub filename: Option<Cow<'a, str>>,
        #[serde(alias = "codeId", default)]
        #[allow(unused)]
        pub code_id: Option<Cow<'a, str>>,
        #[serde(alias = "debugFile", default)]
        pub debug_file: Option<Cow<'a, str>>,
        #[serde(alias = "debugId", default)]
        pub debug_id: Option<Cow<'a, str>>,
        #[serde(default)]
        #[allow(unused)]
        pub version: Option<Cow<'a, str>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct Thread<'a> {
        #[serde(borrow, default)]
        pub frames: Vec<Frame<'a>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct Frame<'a> {
        pub ip: Cow<'a, str>,
        #[serde(alias = "moduleIndex", default)]
        pub module_index: Option<usize>,
        #[allow(unused)]
        pub trust: Cow<'a, str>,
    }
}
