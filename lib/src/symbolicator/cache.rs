//! This cache API implements a specific flow where the cache waits until it has total
//! information about batch work before evicting anything. This allows the cache to retain files
//! until all jobs which may need them either use them, or are dropped.
//!
//! In particular, the API here enforces the following pattern:
//! 1. Load _all_ jobs and register what files they may use. Once each job is done registering,
//!    they can further interact with the cache when fetching files by reserving space.
//! 2. Once all jobs are loaded, evictions may begin (when possible).
//!
//! This interface does _not_ deal with the cached files themselves: it is only responsible for the
//! logical state of the cache.

use debugid::DebugId;
use std::collections::BinaryHeap;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
    Arc, Mutex,
};
use tokio::sync::Notify;

pub struct Cache {
    state: Arc<State>,
}

#[derive(Clone)]
pub struct CacheJob {
    state: Arc<State>,
}

impl Cache {
    pub fn new(limit: Option<u64>, persistent_storage: std::path::PathBuf) -> (Self, Vec<Key>) {
        let persisted: PersistedState = std::fs::File::open(&persistent_storage)
            .ok()
            .and_then(|r| serde_json::from_reader(r).ok())
            .unwrap_or_default();
        let keys = persisted.iter().map(|e| e.key.clone()).collect();
        let current_cache_size = persisted.iter().map(|e| e.size).sum();
        let state = Arc::new(State {
            entries: Mutex::new(persisted.into_iter().collect()),
            entries_change: Default::default(),
            limit: limit.map(|l| CacheLimit::new(l, current_cache_size)),
            persist: persistent_storage,
        });
        (Cache { state }, keys)
    }

    pub fn update_status(&self, status: &Arc<crate::Status>) {
        if let Some(cache) = &status.cache {
            let weak_state = Arc::downgrade(&self.state);
            cache.set_current_callback(move || {
                weak_state
                    .upgrade()
                    .and_then(|state| {
                        state
                            .limit
                            .as_ref()
                            .map(|limit| limit.current.load(Relaxed))
                    })
                    .unwrap_or_default()
            });
        }
    }

    pub fn job(&self) -> CacheJob {
        CacheJob {
            state: self.state.clone(),
        }
    }

    pub fn evictor(&self) -> Option<CacheEvictor> {
        if self.state.limit.is_some() {
            Some(CacheEvictor {
                state: self.state.clone(),
            })
        } else {
            None
        }
    }
}

pub struct CacheEvictor {
    state: Arc<State>,
}

impl CacheEvictor {
    pub async fn evict<F: FnOnce(Key)>(&self, remove: F) -> bool {
        let limit = self.state.limit.as_ref().unwrap();
        let waiters_changed = limit.waiting_for_space.change();
        if !limit.waiting_for_space.has_waiters() {
            return false;
        }

        tokio::select! {
            _ = waiters_changed => true,
            _ = self.state.evict(remove) => {
                // Give waiters some time to access the newly-available cache space and clear their
                // wait tokens. Otherwise, the eviction task may quickly run through and evict
                // everthing before all the waiters have a chance to clear and acquire the space. We
                // don't want to clear everything, since some things are faster to keep around (e.g.
                // symbol maps for release binaries).
                //
                // There is a complicated way to do this without a sleep, but the sleep is good
                // enough.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                true
            }
        }
    }
}

impl CacheJob {
    pub fn register(&self, file: Key, channel: Channel) -> PendingCacheEntry {
        PendingCacheEntry {
            state: self.state.clone(),
            entry: Some(Entry::new(file, channel)),
        }
    }
}

struct CacheLimit {
    limit: u64,
    current: AtomicU64,
    waiting_for_space: WaitingForSpace,
}

#[derive(Default)]
struct WaitingForSpace {
    waiters: AtomicUsize,
    notify: Notify,
}

struct WaitingForSpaceWaiter<'a> {
    inner: &'a WaitingForSpace,
}

impl<'a> WaitingForSpaceWaiter<'a> {
    fn new(inner: &'a WaitingForSpace) -> Self {
        inner.waiters.fetch_add(1, Relaxed);
        WaitingForSpaceWaiter { inner }
    }

    async fn wait(&self) {
        self.inner.notify.notified().await;
    }
}

impl<'a> Drop for WaitingForSpaceWaiter<'a> {
    fn drop(&mut self) {
        self.inner.waiters.fetch_sub(1, Relaxed);
    }
}

impl WaitingForSpace {
    fn waiter(&self) -> WaitingForSpaceWaiter<'_> {
        WaitingForSpaceWaiter::new(self)
    }

    async fn change(&self) {
        self.notify.notified().await;
    }

    fn notify(&self) {
        self.notify.notify_waiters();
    }

    fn has_waiters(&self) -> bool {
        self.waiters.load(Relaxed) > 0
    }
}

impl CacheLimit {
    fn new(limit: u64, current: u64) -> Self {
        CacheLimit {
            limit,
            current: current.into(),
            waiting_for_space: Default::default(),
        }
    }

    async fn wait_for_space(&self, amount: u64) {
        let check_amount = std::cmp::min(amount, self.limit);
        let mut waiter = None;
        while self
            .current
            .fetch_update(Relaxed, Relaxed, |v| {
                (v + check_amount <= self.limit).then_some(v + amount)
            })
            .is_err()
        {
            waiter
                .get_or_insert_with(|| self.waiting_for_space.waiter())
                .wait()
                .await;
        }
    }

    fn take_space(&self, amount: u64) {
        self.current.fetch_add(amount, Relaxed);
    }

    fn return_space(&self, amount: u64) {
        self.current.fetch_sub(amount, Relaxed);
        self.waiting_for_space.notify();
    }
}

struct State {
    entries: Mutex<BinaryHeap<Entry>>,
    entries_change: Notify,
    limit: Option<CacheLimit>,
    persist: std::path::PathBuf,
}

impl State {
    async fn evict<F: FnOnce(Key)>(&self, remove: F) {
        loop {
            let to_evict = self.entries.lock().ok().and_then(|mut bh| bh.pop());
            if let Some(entry) = to_evict {
                log::debug!("evicting {}", entry.key);
                self.remove_entry(entry, remove);
                return;
            }
            self.entries_change.notified().await;
        }
    }

    fn remove_entry<F: FnOnce(Key)>(&self, entry: Entry, remove: F) {
        let size = entry.size;
        remove(entry.key);
        if let Some(limit) = &self.limit {
            limit.return_space(size);
        }
    }

    fn insert_entry(&self, entry: Entry) {
        if let Ok(mut guard) = self.entries.lock() {
            guard.push(entry);
        }
        self.entries_change.notify_waiters();
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let entries: PersistedState = self
            .entries
            .get_mut()
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .collect();
        if !entries.is_empty() {
            if let Some(p) = self.persist.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            match std::fs::File::create(&self.persist) {
                Ok(f) => {
                    if let Err(e) = serde_json::to_writer(f, &entries) {
                        log::error!("failed to persist cache state: {e}");
                    }
                }
                Err(e) => log::error!("failed to create persistent cache state file: {e}"),
            }
        }
    }
}

type PersistedState = Vec<Entry>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Key {
    pub debug_name: String,
    pub debug_id: DebugId,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.debug_id.breakpad(), self.debug_name)
    }
}

pub struct PendingCacheEntry {
    state: Arc<State>,
    entry: Option<Entry>,
}

impl PendingCacheEntry {
    /// Reserve space in the cache for this entry. If there is not enough room, the future will
    /// suspend.
    pub async fn reserve_space(&mut self, size: u64) {
        let entry = self.entry.as_mut().expect("entry already committed");
        if let Some(limit) = &self.state.limit {
            let current = entry.size;
            if size < current {
                limit.return_space(current - size);
            } else if size > current {
                limit.wait_for_space(size - current).await;
            }
        }
        entry.size = size;
    }

    /// Reserve space in the cache for this entry, regardless of the cache size limit (useful for
    /// files that already exist).
    pub fn exists_with_space(&mut self, size: u64) {
        let mut entry = self.entry.take().expect("entry already committed");
        if let Some(limit) = &self.state.limit {
            let current = entry.size;
            if size < current {
                limit.return_space(current - size);
            } else if size > current {
                limit.take_space(size - current);
            }
        }
        entry.size = size;
        self.state.insert_entry(entry);
    }
}

impl Drop for PendingCacheEntry {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            // The entry was never committed, so remove it.
            self.state.remove_entry(entry, |_| ());
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Entry {
    key: Key,
    channel: Channel,
    /// The size of the debug file data on disk. 0 if unknown.
    size: u64,
}

impl Entry {
    fn new(key: Key, channel: Channel) -> Self {
        Entry {
            key,
            channel,
            size: 0,
        }
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.channel == other.channel && self.size == other.size
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Lesser channels (e.g. nightly) are ordered first
        self.channel
            .cmp(&other.channel)
            // Larger sizes are ordered first (greedy eviction)
            .then_with(|| self.size.cmp(&other.size).reverse())
            // Reverse for usage in BinaryHeap
            .reverse()
    }
}

#[derive(
    Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
pub enum Channel {
    Nightly,
    Beta,
    Unknown,
    Release,
    Esr,
}

impl From<&str> for Channel {
    fn from(value: &str) -> Self {
        match value {
            "nightly" => Channel::Nightly,
            "beta" => Channel::Beta,
            "release" => Channel::Release,
            "esr" => Channel::Esr,
            _ => Channel::Unknown,
        }
    }
}
