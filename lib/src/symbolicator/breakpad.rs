//! Breakpad symbol server utilities.

use download_cache::fs::{CachedFiles, TargetFiles, Transform, Url};
use samply_symbols::BreakpadIndexParser;
use tokio::io::AsyncWriteExt;

/// Build the breakpad urls for the given key.
pub fn get_urls(servers: &[String], key: &super::Key, urls: &mut Vec<Url>) {
    for server in servers {
        urls.push(
            Url::new(format!(
                "{server}/{}/{}/{}.sym",
                key.debug_name,
                key.debug_id.breakpad(),
                key.debug_name.trim_end_matches(".pdb")
            ))
            .with_transform(BuildSymindex),
        );
    }
}

/// Get the symindex path for a cached breakpad file.
pub fn symindex_path(files: &CachedFiles) -> std::path::PathBuf {
    files.associated("symindex")
}

struct BuildSymindex;

#[async_trait::async_trait]
impl Transform for BuildSymindex {
    async fn transform(
        &mut self,
        mut response: reqwest::Response,
        target: &mut TargetFiles,
    ) -> std::io::Result<()> {
        let mut index = BreakpadIndexParser::new();

        let file = target.primary();
        loop {
            match response.chunk().await {
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "error getting response: {e}"
                    )));
                }
                Ok(Some(chunk)) => {
                    file.write_all(&chunk).await?;
                    index.consume(&chunk);
                }
                Ok(None) => break,
            }
        }

        let symindex_result: anyhow::Result<()> = (async {
            let symindex = target.associated("symindex".to_owned())?;
            let index = index.finish()?;
            symindex.write_all(&index.serialize_to_bytes()).await?;
            Ok(())
        })
        .await;

        if let Err(e) = symindex_result {
            log::error!("failed to generate breakpad symindex file: {e:#}");
        }

        Ok(())
    }
}
