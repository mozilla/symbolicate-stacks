//! Windows symsrv logic.

use anyhow::Context;
use download_cache::fs::{TargetFiles, Transform, Url};
use std::{
    io::{Read, Seek},
    path::PathBuf,
};

/// Build the symsrv urls for the given key.
pub fn get_urls(servers: &[String], key: &super::Key, urls: &mut Vec<Url>) -> anyhow::Result<()> {
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

    for base in servers {
        urls.push(Url::new(url_join(base, uncompressed_path.components())));
        urls.push(
            Url::new(url_join(base, compressed_path.components())).with_transform(CabExtractFile),
        );
    }

    Ok(())
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

struct CabExtractFile;

#[async_trait::async_trait]
impl Transform for CabExtractFile {
    async fn transform(
        &mut self,
        response: reqwest::Response,
        target: &mut TargetFiles,
    ) -> std::io::Result<()> {
        let content = response
            .bytes()
            .await
            .map_err(|e| std::io::Error::other(e))?;
        let mut stdfile = target.primary().try_clone().await?.into_std().await;
        tokio::task::spawn_blocking(move || {
            extract_cab_to_file(std::io::Cursor::new(content.as_ref()), &mut stdfile)
        })
        .await??;
        Ok(())
    }
}

fn get_first_file_entry<R: Read + Seek>(cabinet: &mut cab::Cabinet<R>) -> Option<(String, u64)> {
    for folder in cabinet.folder_entries() {
        if let Some(file) = folder.file_entries().next() {
            return Some((file.name().to_owned(), file.uncompressed_size().into()));
        }
    }
    None
}

// Cabinet is not `Sync`, so we can't benefit from async within this function.
fn extract_cab_to_file<R: Read + Seek>(
    source_data: R,
    dest_file: &mut std::fs::File,
) -> std::io::Result<u64> {
    use std::io::Write;

    let mut cabinet = cab::Cabinet::new(source_data).map_err(|e| std::io::Error::other(e))?;
    let Some((file_entry_name, _file_extracted_size)) = get_first_file_entry(&mut cabinet) else {
        return Err(std::io::Error::other("empty cab file"));
    };
    let mut reader = cabinet.read_file(&file_entry_name)?;

    let mut bytes_written = 0;
    loop {
        let mut buf = [0; 4096];
        let bytes_read = reader.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        dest_file.write_all(&buf[..bytes_read])?;
        bytes_written += bytes_read as u64;
    }

    Ok(bytes_written)
}
