//! Server-related functionality.

use anyhow::Context;
use interprocess::local_socket::{
    GenericFilePath, ListenerOptions, Name,
    tokio::{Stream, prelude::*},
    traits::Stream as _,
};
use lib::SymbolicateStacks;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Notify,
};

fn runtime_dir() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .map(|s| s.into())
        .unwrap_or_else(|_| std::env::temp_dir())
}

fn pid_path() -> PathBuf {
    runtime_dir().join("symbolicate-stacks.pid")
}

fn local_socket_path() -> PathBuf {
    runtime_dir().join("symbolicate-stacks.sock")
}

fn local_socket_name() -> Name<'static> {
    local_socket_path().to_fs_name::<GenericFilePath>().unwrap()
}

pub fn exists() -> Option<u32> {
    if let Ok(mut file) = std::fs::File::open(pid_path()) {
        if let Err(std::fs::TryLockError::WouldBlock) = file.try_lock_shared() {
            let mut s = String::new();
            file.read_to_string(&mut s).ok()?;
            s.parse().ok()
        } else {
            None
        }
    } else {
        None
    }
}

pub fn connect() -> std::io::Result<interprocess::local_socket::Stream> {
    interprocess::local_socket::Stream::connect(local_socket_name())
}

pub fn ping() -> std::io::Result<()> {
    let mut conn = connect()?;
    conn.write_all(b"ping\0")?;
    let mut response = Vec::new();
    conn.read_to_end(&mut response)?;
    if response != b"pong" {
        Err(std::io::Error::other("unexpected response"))
    } else {
        Ok(())
    }
}

pub fn close() -> std::io::Result<()> {
    connect()?.write_all(b"close")
}

async fn handle_connection(
    mut conn: Stream,
    symbolicator: &SymbolicateStacks,
) -> anyhow::Result<bool> {
    log::debug!("handling connection {conn:?}");
    let mut input = Vec::new();
    BufReader::new(&mut conn).read_until(0, &mut input).await?;
    if matches!(input.last(), Some(0)) {
        input.pop();
    }
    let input = String::from_utf8(input)?;

    if input == "close" {
        return Ok(false);
    } else if input == "ping" {
        conn.write_all(b"pong").await?;
        return Ok(true);
    }

    log::debug!("symbolicating connection {conn:?}");

    let (status, result) = match symbolicator.symbolicate(input).await {
        Ok(v) => (b'o', v),
        Err(e) => (b'e', e.to_string()),
    };
    log::debug!(
        "finished symbolicating connection {conn:?} ({})",
        if status == b'o' { "success" } else { "errored" }
    );
    conn.write_all(&[status]).await?;
    conn.write_all(result.as_bytes()).await?;

    Ok(true)
}

fn acquire_pid_lock_on(path: &Path) -> anyhow::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .context("failed to open pid path")?;

    f.try_lock().context("failed to acquire exclusive lock")?;

    write!(f, "{}", std::process::id()).context("failed to write pid")?;

    // Hold the file open for the duration of the process, which holds the lock.
    std::mem::forget(f);
    Ok(())
}

pub fn acquire_pid_lock() -> anyhow::Result<()> {
    let pid_path = pid_path();
    acquire_pid_lock_on(&pid_path)
        .with_context(|| format!("failed to acquire pid lock on {}", pid_path.display()))
}

pub async fn run(symbolicator: &Arc<SymbolicateStacks>) {
    let listener = ListenerOptions::new()
        .name(local_socket_name())
        .create_tokio()
        .expect("failed to create socket listener");

    log::info!(
        "listening to connections on {}",
        local_socket_path().display()
    );

    let exit = Arc::new(Notify::new());

    {
        let exit = exit.clone();
        if let Err(e) = ctrlc::set_handler(move || exit.notify_one()) {
            log::warn!("failed to set signal handler: {e}");
        }
    }

    loop {
        log::trace!("waiting on connection");
        let conn = tokio::select! {
               result = listener.accept() => match result {
                   Ok(c) => c,
                   Err(e) => {
                       log::error!("failed to accept connection: {e}");
                       continue;
                   }
               },
               _ = exit.notified() => break,
        };
        log::debug!("accepted connection {conn:?}");

        let symbolicator = symbolicator.clone();
        let exit = exit.clone();

        tokio::spawn(async move {
            match handle_connection(conn, &symbolicator).await {
                Ok(should_continue) => {
                    if !should_continue {
                        exit.notify_one();
                    }
                }
                Err(e) => log::error!("error handling connection: {e}"),
            }
        });
    }
}
