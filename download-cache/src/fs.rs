//! A filesystem-based download cache.
//!
//! This cache is suitable for use among multiple concurrent processes. It does not store additional
//! state about downloads (e.g. 404s), file size, etc., to keep concurrent access simple and fast.
//!
//! ## Eviction
//! Because there is no additional stored state, it would be somewhat inefficient for short-lived
//! processes to try to maintain a cache size limit: it would involve reading file attributes of all
//! cached files to determine if eviction were necessary and what to evict. However, to support
//! eviction (if desired), the cache is designed such that it will still be in a consistent state if
//! a file is removed. Therefore, if cache clients want to support eviction, **they must also permit
//! files to be arbitrarily removed**.
//!
//! The `get` method of the cache will update the access and modification times of the cached file
//! (if any), as if the file were downloaded at that point. This information may be used for
//! eviction (e.g. LRU semantics).
//!
//! The `used` method allows clients to manually update the access and modification times for an
//! entry.
//!
//! The `evict` function will evict the oldest files until the disk space used is below the given
//! threshold.
//!
//! ## Transforms
//! The cache supports arbitrary transforms on the downloaded data before storage. Use this for e.g.
//! extraction or decompression, when necessary. Transforms also allow storing associated files
//! (e.g., for index files). Note that `get()` will always return the target path regardless of
//! whether associated files exist or not, so they should be used for optional information.
//!
//! ## Storage
//! Files are stored in a directory with the cache key path appended, e.g.
//! `<CACHEDIR>/THE/CACHE/KEY/_primary` for the primary file. Associated files, if any, are stored
//! in the same directory.
//!

use crate::http::retry_requests;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tempfile::NamedTempFile;
use tokio::fs::File;

const TMP_SUBDIR_NAME: &str = ".tmp";
const PRIMARY_FILE_SUBDIR_NAME: &str = "_primary";

/// An offline filesystem download cache.
pub struct OfflineFsCache {
    tempdir: PathBuf,
}

/// A filesystem download cache.
pub struct FsCache {
    cache: OfflineFsCache,
    client: reqwest::Client,
}

impl std::ops::Deref for FsCache {
    type Target = OfflineFsCache;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

/// Target files of a transform operation.
pub struct TargetFiles {
    tempdir: PathBuf,
    primary: TempFile,
    associated: Vec<(String, TempFile)>,
}

impl TargetFiles {
    fn new_in(tempdir: PathBuf) -> std::io::Result<Self> {
        let primary = TempFile::new_in(&tempdir)?;
        Ok(Self {
            tempdir,
            primary,
            associated: Default::default(),
        })
    }

    /// Get the primary target file.
    pub fn primary(&mut self) -> &mut File {
        self.primary.as_file_mut()
    }

    /// Get a path for an associated target file.
    ///
    /// The caller must not call this method more than once with the same name.
    pub fn associated(&mut self, name: String) -> std::io::Result<&mut File> {
        if name == PRIMARY_FILE_SUBDIR_NAME {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidFilename,
                format!(
                    "cannot use {} as an associated file name",
                    PRIMARY_FILE_SUBDIR_NAME
                ),
            ));
        }
        let file = TempFile::new_in(&self.tempdir)?;
        self.associated.push((name, file));
        Ok(self.associated.last_mut().unwrap().1.as_file_mut())
    }
}

/// Files stored in the cache for a given key.
pub struct CachedFiles {
    dir: PathBuf,
}

impl CachedFiles {
    fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// The primary file for the key.
    pub fn primary(&self) -> PathBuf {
        self.dir.join(PRIMARY_FILE_SUBDIR_NAME)
    }

    /// Get an associated file for the key.
    pub fn associated(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

/// Transforms which can apply to a response and store a file.
#[async_trait::async_trait]
pub trait Transform {
    /// Transform the response from a URL into the target file.
    async fn transform(
        &mut self,
        response: reqwest::Response,
        target: &mut TargetFiles,
    ) -> std::io::Result<()>;
}

/// A URL source for a file to cache.
pub struct Url {
    url: String,
    transform: Option<Box<dyn Transform + Send>>,
}

impl Url {
    /// Create a new URL source.
    pub fn new(url: String) -> Self {
        Self {
            url,
            transform: None,
        }
    }

    /// Apply a transform to the URL response.
    ///
    /// The transform is responsible for populating the cache file.
    pub fn with_transform<T: Transform + Send + 'static>(mut self, t: T) -> Self {
        self.transform = Some(Box::new(t));
        self
    }
}

impl From<String> for Url {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for Url {
    fn from(s: &str) -> Self {
        Self::new(s.to_owned())
    }
}

/// Types which can be used as cache keys.
pub trait CacheKey {
    /// Return the cache key for this value.
    ///
    /// The cache key can have multiple Path components. Key components cannot be prefixes of other
    /// keys used in the same cache. For example, if one key is `foo/bar`, another key cannot be
    /// `foo`.
    fn cache_key(&self) -> PathBuf;
}

impl OfflineFsCache {
    /// Manage an offline-only download cache in the given directory.
    pub fn new(dir: PathBuf) -> Self {
        let tempdir = dir.join(TMP_SUBDIR_NAME);
        Self { tempdir }
    }

    /// Get the download cache directory.
    pub fn dir(&self) -> &Path {
        self.tempdir.parent().unwrap()
    }

    /// Check whether the cache contains an entry for the given key.
    pub fn contains(&self, key: &impl CacheKey) -> bool {
        // Use the primary file to check for existence, as this is moved in last when persisting entries.
        self.key_to_path(key)
            .join(PRIMARY_FILE_SUBDIR_NAME)
            .exists()
    }

    /// Get files for the given cache key, if they already exist.
    ///
    /// Note that there is no guarantee that the paths still exists after return from this function
    /// (e.g. if cache eviction is also employed).
    ///
    /// Returns `None` if the files could not be retrieved from the given urls.
    pub fn get_offline(&self, key: &impl CacheKey) -> Option<CachedFiles> {
        if self.contains(key) {
            self.used(key);
            Some(CachedFiles::new(self.key_to_path(key)))
        } else {
            None
        }
    }

    /// Mark the given cache key as used at this point in time.
    pub fn used(&self, key: &impl CacheKey) {
        use std::fs::{File, FileTimes, read_dir};

        fn set_file_times(path: &Path, time: SystemTime) {
            match File::open(path) {
                Ok(f) => {
                    let times = FileTimes::new().set_accessed(time).set_modified(time);
                    if let Err(e) = f.set_times(times) {
                        log::error!("failed to update file times for {}: {e}", path.display());
                    }
                }
                Err(e) => {
                    log::error!(
                        "failed to open file to update file times for {}: {e}",
                        path.display()
                    );
                }
            }
        }

        tokio::task::block_in_place(|| {
            let now = SystemTime::now();
            let dir = self.key_to_path(key);
            let read_dir = match read_dir(&dir) {
                Err(e) => {
                    log::error!("failed to read dir {}: {e}", dir.display());
                    return;
                }
                Ok(r) => r,
            };
            // Set file times for all files in the entry directory
            for entry in read_dir {
                match entry {
                    Err(e) => {
                        log::error!("failed to read dir {}: {e}", dir.display());
                        return;
                    }
                    Ok(e) => set_file_times(&e.path(), now),
                };
            }
        })
    }

    /// Remove the given cache key entry.
    ///
    /// Any associated files will also be removed.
    pub async fn remove(&self, key: &impl CacheKey) -> std::io::Result<()> {
        self.remove_subpath(&self.dir().join(key.cache_key())).await
    }

    /// Remove a path that is a subpath of the cache directory.
    async fn remove_subpath(&self, path: &Path) -> std::io::Result<()> {
        let root_dir = self.dir();
        if path.ancestors().position(|p| p == root_dir).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path not a subpath of the cache directory",
            ));
        }

        if path.is_dir() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }

        // Remove empty parent directories
        let mut ancestors = path.ancestors();
        // Skip the path itself
        ancestors.next();
        for dir in ancestors {
            if dir == root_dir {
                break;
            }
            if let Err(e) = tokio::fs::remove_dir(&dir).await {
                if e.kind() != std::io::ErrorKind::DirectoryNotEmpty {
                    log::warn!("failed to remove ancestor directory {}: {e}", dir.display());
                    break;
                }
            }
        }
        Ok(())
    }

    /// Evict cached files, leaving files at most totalling `max_bytes`.
    ///
    /// Eviction removes files in least-recently-modified order.
    pub async fn evict(&self, max_bytes: u64) -> std::io::Result<()> {
        let mut cache_entries = enumerate_cache_entries(self.dir()).await;

        let total_bytes = cache_entries.iter().map(|a| a.2).sum::<u64>();
        if total_bytes <= max_bytes {
            return Ok(());
        }
        let overflow = total_bytes - max_bytes;
        cache_entries.sort_unstable_by(|a, b| a.1.cmp(&b.1));

        let to_remove = cache_entries.into_iter().scan(0, |sum, (path, _, size)| {
            if *sum >= overflow {
                return None;
            }
            *sum += size;
            Some(path)
        });

        for path in to_remove {
            self.remove_subpath(&path).await?;
        }

        Ok(())
    }

    /// Get the path for with the given key.
    fn key_to_path(&self, key: &impl CacheKey) -> PathBuf {
        self.dir().join(key.cache_key())
    }
}

impl FsCache {
    /// Manage a download cache in the given directory.
    pub fn new(dir: PathBuf, client: reqwest::Client) -> Self {
        let cache = Self::offline(dir);
        Self { cache, client }
    }

    /// Manage an offline-only download cache in the given directory.
    ///
    /// This is useful for `evict()` or `used()` without creating a client.
    pub fn offline(dir: PathBuf) -> OfflineFsCache {
        OfflineFsCache::new(dir)
    }

    /// Get files for the given cache key, trying each of the given urls.
    ///
    /// Note that there is no guarantee that the paths still exists after return from this function
    /// (e.g. if cache eviction is also employed).
    ///
    /// Returns `None` if the files could not be retrieved from the given urls.
    pub async fn get(
        &self,
        key: &impl CacheKey,
        urls: Vec<Url>,
    ) -> std::io::Result<Option<CachedFiles>> {
        if let Some(files) = self.get_offline(key) {
            return Ok(Some(files));
        }

        let path = self.key_to_path(key);
        for Url { url, transform } in urls {
            match self.get_url(&url, transform).await {
                Err(e) => {
                    log::debug!("failed to fetch {url}: {e}");
                    continue;
                }
                Ok(files) => {
                    let TargetFiles {
                        primary,
                        associated,
                        ..
                    } = files;

                    tokio::fs::create_dir_all(&path).await?;

                    for (name, f) in associated {
                        if let Err(e) = f.persist_noclobber(&path.join(&name)) {
                            if e.error.kind() != std::io::ErrorKind::AlreadyExists {
                                log::warn!(
                                    "failed to persist associated file ({name}) to {}: {e}",
                                    path.display()
                                );
                            }
                        }
                    }

                    // Move in the primary file last, as we use that to check for cache entry
                    // existence.
                    let primary_path = path.join(PRIMARY_FILE_SUBDIR_NAME);
                    if let Err(e) = primary.persist_noclobber(&primary_path) {
                        if e.error.kind() != std::io::ErrorKind::AlreadyExists {
                            log::warn!("failed to persist to {}: {e}", primary_path.display());
                            return Err(e.error);
                        }
                    }

                    return Ok(Some(CachedFiles::new(path)));
                }
            }
        }

        Ok(None)
    }

    async fn get_url(
        &self,
        url: &str,
        transform: Option<Box<dyn Transform + Send>>,
    ) -> Result<TargetFiles, GetError> {
        let response = retry_requests(self.client.get(url)).await?;

        let mut target_files = TargetFiles::new_in(self.tempdir.clone())?;
        if let Some(mut transform) = transform {
            transform.transform(response, &mut target_files).await?;
        } else {
            copy_response(response, target_files.primary()).await?;
        }
        Ok(target_files)
    }
}

/// Get all cache keys with modification time and size in the given cache directory.
async fn enumerate_cache_entries(dir: &Path) -> Vec<(PathBuf, Option<SystemTime>, u64)> {
    let files = get_files(dir).await;

    // First find all keys, so we can group children in the second pass.
    let keys_from_files = files.iter().filter_map(|(p, _, _)| {
        if p.file_name().unwrap() == PRIMARY_FILE_SUBDIR_NAME {
            p.parent().map(ToOwned::to_owned)
        } else {
            None
        }
    });

    let mut keys: HashMap<PathBuf, (Option<SystemTime>, u64)> =
        keys_from_files.map(|k| (k, (None, 0))).collect();

    for (path, mtime, size) in files {
        // Sum the sizes and use the primary file as the modification time.
        if let Some(entry) = keys.get_mut(path.parent().unwrap()) {
            if path.file_name().unwrap() == PRIMARY_FILE_SUBDIR_NAME {
                entry.0 = mtime;
            }
            entry.1 += size;
        } else {
            log::debug!(
                "ignoring file in cache directory that is not in the expected file layout: {}",
                path.display()
            );
        }
    }

    keys.into_iter()
        .map(|(p, (mtime, size))| (p, mtime, size))
        .collect()
}

/// Get all files with modification time and size in the given directory, recursively.
///
/// This will ignore subdirectories named the same as the temporary directory used by `FsCache`.
async fn get_files(dir: &Path) -> Vec<(PathBuf, Option<SystemTime>, u64)> {
    let mut dirs = vec![dir.to_owned()];
    let mut files: Vec<(PathBuf, Option<SystemTime>, u64)> = Vec::new();

    'next_dir: while let Some(dir_path) = dirs.pop() {
        let mut dir = match tokio::fs::read_dir(&dir_path).await {
            Err(e) => {
                log::warn!("failed to read entries in {}: {e}", dir_path.display());
                continue;
            }
            Ok(d) => d,
        };

        loop {
            match dir.next_entry().await {
                Err(e) => {
                    log::warn!("failed to read next entry in {}: {e}", dir_path.display());
                    continue 'next_dir;
                }
                Ok(Some(e)) => {
                    let path = e.path();
                    match e.metadata().await {
                        Err(e) => {
                            log::debug!("failed to get metadata for {}: {e}", path.display());
                        }
                        Ok(m) => {
                            let file_type = m.file_type();
                            if file_type.is_dir() && e.file_name() != TMP_SUBDIR_NAME {
                                dirs.push(path);
                            } else if file_type.is_file() {
                                files.push((path, m.modified().ok(), m.len()));
                            } else if file_type.is_symlink() {
                                log::warn!(
                                    "ignoring symlink {} when enumerating files",
                                    path.display()
                                );
                            }
                        }
                    }
                }
                Ok(None) => break,
            }
        }
    }

    files
}

/// Wrap NamedTempFile<tokio::fs::File> to ensure good async behavior.
///
/// This uses `tokio::task::block_in_place` as an advisory for tokio, but the blocking code is
/// expected to be fast (otherwise `spawn_blocking` might be more appropriate).
struct TempFile {
    inner: NamedTempFile<File>,
}

impl TempFile {
    fn new_in(dir: &Path) -> std::io::Result<Self> {
        let tempfile = tokio::task::block_in_place(|| NamedTempFile::new_in(dir))?;
        let (file, tempfile) = tempfile.into_parts();
        Ok(Self {
            inner: NamedTempFile::from_parts(File::from_std(file), tempfile),
        })
    }

    fn persist_noclobber(
        self,
        new_path: impl AsRef<Path>,
    ) -> Result<File, tempfile::PersistError<File>> {
        tokio::task::block_in_place(move || self.inner.persist_noclobber(new_path))
    }
}

impl std::ops::Deref for TempFile {
    type Target = NamedTempFile<File>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for TempFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

async fn copy_response(mut response: reqwest::Response, target: &mut File) -> Result<(), GetError> {
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = response.chunk().await? {
        target.write_all(&chunk).await?;
    }
    Ok(())
}

/// An error downloading and storing a file.
#[derive(thiserror::Error, Debug)]
pub enum GetError {
    #[error("network error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
}
