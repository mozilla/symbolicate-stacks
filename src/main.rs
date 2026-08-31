use anyhow::Context;
use clap::{Parser, ValueEnum};
use daemonize::{Daemonize, Outcome};
use interprocess::local_socket::{
    GenericFilePath, ListenerOptions, Name,
    tokio::{Stream, prelude::*},
};
use lib::{Config, SymbolicateStacks, config::WorkerThreads};
use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Notify,
};

const DEFAULT_CONFIG_FILE: &str = "config.toml";

fn existing_path(s: &str) -> std::io::Result<PathBuf> {
    let p = PathBuf::from(s);
    if !std::fs::exists(&p)? {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file does not exist",
        ))
    } else {
        Ok(p)
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ServerCommand {
    /// Start the server.
    Start,
    /// Stop the server, if running.
    Stop,
}

/// Crash symbolication.
///
/// All configuration is passed as TOML. In order from lowest to highest precedence: `config_file`
/// and command-line arguments can have TOML data which are merged to determine the final
/// configuration.
#[derive(Debug, Parser)]
struct Args {
    /// The path to the config file. If unspecified, uses `config.toml` (if it exists).
    /// This configuration is only used if the server hasn't been started.
    #[arg(long, value_parser = existing_path)]
    config_file: Option<PathBuf>,

    /// Command the server.
    #[arg(short, long)]
    server: Option<ServerCommand>,

    /// The path from which to read input. If unspecified, input is read from stdin.
    #[arg(short, long)]
    input_file: Option<PathBuf>,

    /// The path to which to write the output. If unspecified, output is written to stdout.
    #[arg(short, long)]
    output_file: Option<PathBuf>,
}

fn runtime_dir() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .map(|s| s.into())
        .unwrap_or_else(|_| std::env::temp_dir())
}

fn log_path() -> PathBuf {
    runtime_dir().join("symbolicate-stacks.err")
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

fn server_exists() -> bool {
    if let Ok(file) = std::fs::File::open(pid_path()) {
        matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
    } else {
        false
    }
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

fn start_server(config_file: PathBuf) -> anyhow::Result<()> {
    let config: Config = std::fs::read_to_string(&config_file)
        .map_err(|e| anyhow::Error::from(e))
        .and_then(|s| Ok(toml::from_str(&s)?))
        .with_context(|| format!("while reading {}", config_file.display()))?;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("symbolicate-stacks");

    if let WorkerThreads::Exact(n) = config.worker_threads {
        builder.worker_threads(n);
    }

    let symbolicator = Arc::new(SymbolicateStacks::new(config)?);

    builder.build()?.block_on(async move {
        let listener = ListenerOptions::new()
            .name(local_socket_name())
            .create_tokio()
            .expect("failed to create socket listener");

        let maintenance_task = {
            let symbolicator = symbolicator.clone();
            tokio::spawn(async move { symbolicator.maintenance().await })
        };

        let exit = Arc::new(Notify::new());
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

        maintenance_task.abort();

        // TODO Wait for any pending tasks to complete (otherwise they will be dropped in whatever
        // intermediate state they were in).
        /*
        let handle = tokio::runtime::Handle::current();
        while handle.metrics().num_alive_tasks() > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        */
    });

    Ok(())
}

fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .parse_default_env()
        .init();

    let args = Args::parse();

    let config_file = args.config_file.unwrap_or(DEFAULT_CONFIG_FILE.into());

    let should_start_server = !matches!(args.server, Some(ServerCommand::Stop));

    if should_start_server && !server_exists() {
        let stderr = std::fs::File::create(log_path()).unwrap();
        let outcome = Daemonize::new()
            .pid_file(pid_path())
            .working_directory(std::env::current_dir().unwrap())
            .stderr(stderr)
            .execute();
        match outcome {
            Outcome::Parent(result) => {
                result.expect("failed to daemonize server");
                if args.server.is_none() {
                    let timeout = std::time::Instant::now() + std::time::Duration::from_secs(1);
                    while !local_socket_path().exists() {
                        if std::time::Instant::now() > timeout {
                            log::error!("daemonized server didn't open socket");
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
            }
            Outcome::Child(child) => {
                child.expect("failed to daemonize server");
                start_server(config_file).expect("failed to start server");
                return Ok(());
            }
        }
    }

    use interprocess::local_socket::traits::Stream;

    match args.server {
        None => {
            let mut input = args
                .input_file
                .map(|path| std::fs::File::open(path).map(|s| Box::new(s) as Box<dyn Read>))
                .unwrap_or_else(|| Ok(Box::new(std::io::stdin()) as Box<dyn Read>))?;

            let mut output = args
                .output_file
                .map(|path| std::fs::File::create(path).map(|f| Box::new(f) as Box<dyn Write>))
                .unwrap_or_else(|| Ok(Box::new(std::io::stdout())))?;

            let mut conn = interprocess::local_socket::Stream::connect(local_socket_name())
                .expect("failed to connect to server");

            std::io::copy(&mut input, &mut conn).expect("failed to write input to server");
            conn.write_all(&[0])
                .expect("failed to write input to server");

            let mut status = [0u8; 1];
            conn.read_exact(&mut status)
                .expect("failed to read from server");

            match status[0] {
                b'o' => std::io::copy(&mut conn, &mut output).expect("failed to read from server"),
                b'e' => std::io::copy(&mut conn, &mut std::io::stderr())
                    .expect("failed to read from server"),
                _ => unreachable!(),
            };
        }
        Some(ServerCommand::Start) => {}
        Some(ServerCommand::Stop) => {
            if server_exists() {
                let mut conn = interprocess::local_socket::Stream::connect(local_socket_name())
                    .expect("failed to connect to server");
                conn.write_all(b"close").expect("failed to write to server");
            }
        }
    }

    Ok(())
}
