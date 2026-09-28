//! Windows symsrv logic.

use super::cache;
use anyhow::Context;
use std::path::{Path, PathBuf};
use symsrv::SymsrvDownloader;

pub struct Symsrv {
    symbol_servers: Vec<String>,
}

pub struct SymsrvUrl {
    pub url: String,
    pub compressed: bool,
}

impl Symsrv {
    pub async fn get_file(
        &self,
        key: &cache::Key,
        cache_entry: &mut cache::PendingCacheEntry,
    ) -> anyhow::Result<std::path::PathBuf> {
        let (target, urls) = self.get_urls(key)?;
    }

    fn get_urls(&self, key: &cache::Key) -> anyhow::Result<(PathBuf, Vec<SymsrvUrl>)> {
        let filename = &key.debug_name;
        let id = key.debug_id.breakpad().to_string();

        let uncompressed_path: PathBuf = [filename, &id, filename].into_iter().collect();

        let ext = uncompressed_path.extension().context("no file extension")?;
        let compressed_ext = match ext.to_string_lossy().as_ref() {
            "exe" => "ex_",
            "dll" => "dl_",
            "pdb" => "pd_",
            "dbg" => "db_",
            _ => anyhow::bail!("unrecognized file extension in {filename}"),
        };

        let compressed_path = {
            let mut p = uncompressed_path.clone();
            p.set_extension(compressed_ext);
            p
        };

        let urls = self
            .symbol_servers
            .iter()
            .flat_map(|base| {
                [
                    SymsrvUrl {
                        url: url_join(base, uncompressed_path.components()),
                        compressed: false,
                    },
                    SymsrvUrl {
                        url: url_join(base, compressed_path.components()),
                        compressed: true,
                    },
                ]
            })
            .collect();

        Ok((uncompressed_path, urls))
    }
}

/// Convert path components into a URL by appending to the given base.
fn url_join(base_url: &str, components: std::path::Components) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        components
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    )
}

impl Symsrv {
    pub fn new(cache_dir: &std::path::Path, symbol_servers: &crate::config::SymbolServers) -> Self {
        let downloader = SymsrvDownloader::new(
            symbol_servers
                .windows
                .iter()
                .map(|url| symsrv::NtSymbolPathEntry::Chain {
                    dll: "symsrv.dll".into(),
                    cache_paths: vec![symsrv::CachePath::Path(cache_dir.join("windows"))],
                    urls: vec![url.clone()],
                })
                .collect(),
        );
        Symsrv { downloader }
    }

    pub async fn get_file(
        &self,
        key: &cache::Key,
        cache_entry: &mut cache::PendingCacheEntry,
    ) -> anyhow::Result<std::path::PathBuf> {
        // SymsrvObserver doesn't have async methods, so we can't suspend a download from
        // occurring. There's no point in trying to use the detected `total_bytes`: we could do a
        // song and dance to extract that value concurrently and use it, but we'd only want to do
        // that to pause the download until the cache has enough space.
        cache_entry.reserve_space(super::UNKNOWN_SIZE).await;
        let path = self
            .downloader
            .get_file(&key.debug_name, &key.debug_id.breakpad().to_string())
            .await?;
        let len = tokio::fs::metadata(&path)
            .await
            .with_context(|| format!("failed to get metadata for {}", path.display()))?
            .len();
        cache_entry.exists_with_space(len);
        Ok(path)
    }
}
