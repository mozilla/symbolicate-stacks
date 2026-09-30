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
        block_in_place(|| {
            let now = SystemTime::now();
            let dir = self.key_to_path(key);
            let read_dir = match std::fs::read_dir(&dir) {
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

fn block_in_place<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    match tokio::runtime::Handle::current().runtime_flavor() {
        tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Set file modification and access times to the given time.
fn set_file_times(path: &Path, time: SystemTime) {
    use std::fs::{File, FileTimes};
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
        let tempfile = block_in_place(|| {
            std::fs::create_dir_all(dir)?;
            NamedTempFile::new_in(dir)
        })?;
        let (file, tempfile) = tempfile.into_parts();
        Ok(Self {
            inner: NamedTempFile::from_parts(File::from_std(file), tempfile),
        })
    }

    fn persist_noclobber(
        self,
        new_path: impl AsRef<Path>,
    ) -> Result<File, tempfile::PersistError<File>> {
        block_in_place(move || self.inner.persist_noclobber(new_path))
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

#[cfg(test)]
mod test {
    use super::*;

    struct Key<'a>(&'a str);

    impl CacheKey for Key<'_> {
        fn cache_key(&self) -> PathBuf {
            Path::new(self.0).to_owned()
        }
    }

    async fn make_fs(dir: &Path, entries: Vec<(&str, Option<(&str, SystemTime)>)>) {
        use tokio::fs::{create_dir_all, write};

        for (path, file) in entries {
            let path = dir.join(path);
            if let Some((content, time)) = file {
                create_dir_all(path.parent().unwrap()).await.unwrap();
                write(&path, content.as_bytes()).await.unwrap();
                set_file_times(&path, time);
            } else {
                create_dir_all(path).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn get_files() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let time = SystemTime::now();
        let time2 = time + std::time::Duration::from_secs(2);
        make_fs(
            dir,
            vec![
                ("a/b/f1", Some(("f1", time))),
                ("a/b/f2", Some(("f2foo", time))),
                ("c/f3", Some(("f3", time2))),
                ("f4", Some(("f4", time2))),
                (".tmp/f5", Some(("f5", time))),
            ],
        )
        .await;

        let mut result = super::get_files(dir).await;
        result.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            result,
            vec![
                (dir.join("a/b/f1"), Some(time), 2),
                (dir.join("a/b/f2"), Some(time), 5),
                (dir.join("c/f3"), Some(time2), 2),
                (dir.join("f4"), Some(time2), 2),
            ]
        );
    }

    #[tokio::test]
    async fn enumerate_cache_entries() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let time = SystemTime::now();
        let time2 = time + std::time::Duration::from_secs(2);
        make_fs(
            dir,
            vec![
                ("a/b/_primary", Some(("ab", time))),
                ("a/b/alt1", Some(("alt1content", time2))),
                ("a/b/alt2", Some(("alt2morecontent", time2))),
                ("c/d/_primary", Some(("cd", time2))),
                ("e/_primary", Some(("e", time2))),
                ("f/somefile", Some(("somefile", time2))),
                (".tmp/other", Some(("tmp", time))),
            ],
        )
        .await;

        let mut result = super::enumerate_cache_entries(dir).await;
        result.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            result,
            vec![
                (dir.join("a/b"), Some(time), 28),
                (dir.join("c/d"), Some(time2), 2),
                (dir.join("e"), Some(time2), 1),
            ]
        );
    }

    struct TestCache<C> {
        #[allow(unused)]
        dir: tempfile::TempDir,
        cache: C,
    }

    impl<C> TestCache<C> {
        async fn new<F: FnOnce(&Path) -> C>(f: F) -> Self {
            let dir = tempfile::tempdir().unwrap();

            let time = SystemTime::now();
            let time2 = time + std::time::Duration::from_secs(2);
            let time3 = time + std::time::Duration::from_secs(8);
            make_fs(
                dir.path(),
                vec![
                    ("a/b/_primary", Some(("ab", time))),
                    ("a/b/alt1", Some(("alt1content", time))),
                    ("a/b/alt2", Some(("alt2morecontent", time))),
                    ("a/q/_primary", Some(("aq", time2))),
                    ("c/d/_primary", Some(("cd", time2))),
                    ("e/_primary", Some(("e", time3))),
                ],
            )
            .await;

            let cache = f(dir.path());

            Self { dir, cache }
        }
    }

    impl<C> std::ops::Deref for TestCache<C> {
        type Target = C;

        fn deref(&self) -> &Self::Target {
            &self.cache
        }
    }

    async fn test_offline_cache() -> TestCache<OfflineFsCache> {
        TestCache::new(|d| OfflineFsCache::new(d.to_owned())).await
    }

    fn cache_using_server(dir: &Path, server: &mockito::ServerGuard) -> FsCache {
        let client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(server.url()).unwrap())
            .build()
            .unwrap();
        FsCache::new(dir.to_owned(), client)
    }

    async fn test_cache_from_server(server: &mockito::ServerGuard) -> TestCache<FsCache> {
        TestCache::new(|d| cache_using_server(d, server)).await
    }

    async fn test_cache() -> (TestCache<FsCache>, mockito::ServerGuard) {
        let server = mockito::Server::new_async().await;
        let cache = test_cache_from_server(&server).await;
        (cache, server)
    }

    /// Test `OfflineFsCache::contains()`.
    #[tokio::test]
    async fn offline_cache_contains() {
        let cache = test_offline_cache().await;
        assert!(cache.contains(&Key("a/b")));
        assert!(cache.contains(&Key("a/q")));
        assert!(cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));
        assert!(!cache.contains(&Key("f")));
    }

    /// Test `OfflineFsCache::get_offline()`.
    #[tokio::test]
    async fn offline_cache_get() {
        let cache = test_offline_cache().await;

        let ab = cache.get_offline(&Key("a/b")).expect("missing a/b key");
        let cd = cache.get_offline(&Key("c/d")).expect("missing c/d key");
        assert_eq!(ab.primary(), cache.dir().join("a/b/_primary"));
        assert_eq!(ab.associated("alt1"), cache.dir().join("a/b/alt1"));
        assert_eq!(ab.associated("alt2"), cache.dir().join("a/b/alt2"));
        assert_eq!(cd.primary(), cache.dir().join("c/d/_primary"));

        // Remove files externally
        tokio::fs::remove_dir_all(cache.dir().join("a/b"))
            .await
            .unwrap();

        assert!(cache.get_offline(&Key("a/b")).is_none())
    }

    /// Test `OfflineFsCache::remove()`.
    #[tokio::test]
    async fn offline_cache_remove() {
        let cache = test_offline_cache().await;

        cache.remove(&Key("a/b")).await.unwrap();
        assert!(cache.get_offline(&Key("a/b")).is_none());
        assert!(cache.dir().join("a").exists());

        cache.remove(&Key("a/q")).await.unwrap();
        assert!(!cache.dir().join("a").exists());
    }

    /// Test `OfflineFsCache::used()`.
    #[tokio::test]
    async fn offline_cache_used() {
        let cache = test_offline_cache().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let now = SystemTime::now();

        cache.used(&Key("a/b"));

        let meta = tokio::fs::metadata(cache.dir().join("a/b/_primary"))
            .await
            .unwrap();
        assert!(meta.modified().unwrap() >= now);
        assert!(meta.accessed().unwrap() >= now);
    }

    /// Test `OfflineFsCache::evict()`.
    #[tokio::test]
    async fn offline_cache_evict() {
        let cache = test_offline_cache().await;
        cache.evict(33).await.unwrap();
        assert!(cache.contains(&Key("a/b")));
        assert!(cache.contains(&Key("a/q")));
        assert!(cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));

        cache.evict(32).await.unwrap();
        assert!(!cache.contains(&Key("a/b")));
        assert!(cache.contains(&Key("a/q")));
        assert!(cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));

        cache.evict(5).await.unwrap();
        assert!(cache.contains(&Key("a/q")));
        assert!(cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));

        cache.evict(4).await.unwrap();
        assert!(cache.contains(&Key("a/q")) ^ cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));

        cache.evict(3).await.unwrap();
        assert!(cache.contains(&Key("a/q")) ^ cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));

        cache.evict(2).await.unwrap();
        assert!(!cache.contains(&Key("a/q")));
        assert!(!cache.contains(&Key("c/d")));
        assert!(cache.contains(&Key("e")));

        cache.evict(1).await.unwrap();
        assert!(cache.contains(&Key("e")));

        cache.evict(0).await.unwrap();
        assert!(!cache.contains(&Key("e")));
    }

    /// Test `FsCache::get()` with a file to download.
    #[tokio::test]
    async fn cache_get() {
        let (cache, mut server) = test_cache().await;
        let m = server
            .mock("GET", "/file")
            .with_body("contents")
            .create_async()
            .await;
        let contents = cache
            .get(&Key("file"), vec![Url::new("http://test/file".into())])
            .await
            .expect("failed to fetch file")
            .expect("failed to get contents");
        assert_eq!(contents.primary(), cache.dir().join("file/_primary"));
        assert_eq!(
            tokio::fs::read(contents.primary()).await.unwrap(),
            b"contents"
        );
        m.assert_async().await;
    }

    /// Test `FsCache::get()` with an existing entry, ensuring no download occurs.
    #[tokio::test]
    async fn cache_get_existing() {
        let (cache, mut server) = test_cache().await;
        let m = server
            .mock("GET", "/file")
            .with_body("contents")
            .expect(0)
            .create_async()
            .await;
        let contents = cache
            .get(&Key("a/b"), vec![Url::new("http://test/file".into())])
            .await
            .expect("failed to fetch file")
            .expect("failed to get contents");
        assert_eq!(contents.primary(), cache.dir().join("a/b/_primary"));
        assert_eq!(tokio::fs::read(contents.primary()).await.unwrap(), b"ab");
        m.assert_async().await;
    }

    /// Test `FsCache::get()` when there is no entry and a 404 from the server.
    #[tokio::test]
    async fn cache_get_missing() {
        let (cache, _server) = test_cache().await;
        let contents = cache
            .get(&Key("file"), vec![Url::new("http://test/file".into())])
            .await
            .expect("failed to fetch file");
        assert!(contents.is_none());
    }

    /// Test that `FsCache::get()` will attempt urls in order and move to the next upon failure.
    #[tokio::test]
    async fn cache_get_fallback() {
        let (cache, mut server) = test_cache().await;
        let file = server
            .mock("GET", "/file")
            .with_body("contents")
            .expect(0)
            .create_async()
            .await;
        let file2 = server
            .mock("GET", "/file2")
            .with_body("2")
            .create_async()
            .await;
        let contents = cache
            .get(
                &Key("file"),
                vec![
                    Url::new("http://test/doesnotexist".into()),
                    Url::new("http://test/doesnotexistagain".into()),
                    Url::new("http://test/file2".into()),
                    Url::new("http://test/file".into()),
                    Url::new("http://test/doesnotexist3".into()),
                ],
            )
            .await
            .expect("failed to fetch file")
            .expect("failed to get contents");
        assert_eq!(contents.primary(), cache.dir().join("file/_primary"));
        assert_eq!(tokio::fs::read(contents.primary()).await.unwrap(), b"2");
        file.assert_async().await;
        file2.assert_async().await;
    }

    /// Test `FsCache::get()` when a transform is provided.
    ///
    /// This also tests the associated files behavior.
    #[tokio::test]
    async fn cache_get_transform() {
        struct TestTransform;

        #[async_trait::async_trait]
        impl Transform for TestTransform {
            async fn transform(
                &mut self,
                response: reqwest::Response,
                target: &mut TargetFiles,
            ) -> std::io::Result<()> {
                use tokio::io::AsyncWriteExt;
                let r = response.headers().get("x-hello").unwrap();

                let hello = target.associated("hello".into())?;
                hello.write_all(r.as_bytes()).await?;

                let body = response.bytes().await.map_err(std::io::Error::other)?;
                let primary = target.primary();
                primary.write_all(body.as_ref()).await?;
                primary.write_all(b" and extra").await?;

                Ok(())
            }
        }

        let (cache, mut server) = test_cache().await;
        let m = server
            .mock("GET", "/file")
            .with_body("contents")
            .with_header("x-hello", "world")
            .create_async()
            .await;
        let contents = cache
            .get(
                &Key("file"),
                vec![Url::new("http://test/file".into()).with_transform(TestTransform)],
            )
            .await
            .expect("failed to fetch file")
            .expect("failed to get contents");
        assert_eq!(
            tokio::fs::read(contents.primary()).await.unwrap(),
            b"contents and extra"
        );
        assert_eq!(
            tokio::fs::read(contents.associated("hello")).await.unwrap(),
            b"world"
        );
        m.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_concurrent_get() {
        // Add a delay in `transform` to make it very likely that tasks don't see other successes
        // and download/store concurrently.
        struct Delay;
        #[async_trait::async_trait]
        impl Transform for Delay {
            async fn transform(
                &mut self,
                response: reqwest::Response,
                target: &mut TargetFiles,
            ) -> std::io::Result<()> {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if let Err(e) = copy_response(response, target.primary()).await {
                    return Err(match e {
                        GetError::IO(e) => e,
                        GetError::Reqwest(r) => std::io::Error::other(r),
                    });
                }
                Ok(())
            }
        }

        const CONCURRENT_COUNT: usize = 50;
        const FILE_COUNT: usize = 4;

        assert!(FILE_COUNT <= CONCURRENT_COUNT);

        let (cache, mut server) = test_cache().await;

        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_body_from_request(|request| request.path().as_bytes().to_vec())
            .expect_at_least(FILE_COUNT)
            .create_async()
            .await;

        let mut tasks = tokio::task::JoinSet::new();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(CONCURRENT_COUNT));
        for i in 0..CONCURRENT_COUNT {
            let cache = cache_using_server(cache.dir(), &server);
            let file = format!("file{}", i % FILE_COUNT);
            let url = format!("http://test/{file}");
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                cache
                    .get(&Key(&file), vec![Url::new(url).with_transform(Delay)])
                    .await
                    .expect("failed to fetch file")
                    .expect("failed to get contents");
            });
        }

        tasks.join_all().await;
        mock.assert_async().await;

        for i in 0..FILE_COUNT {
            let contents = cache
                .get_offline(&Key(&format!("file{i}")))
                .expect("file missing");
            assert_eq!(
                tokio::fs::read(contents.primary()).await.unwrap(),
                format!("/file{i}").as_bytes()
            );
        }
    }
}
