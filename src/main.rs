use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use lib::{Config, SymbolicateStacks, config::WorkerThreads};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

mod server;

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

/// The operating mode of the program.
#[derive(Debug, Subcommand)]
enum Command {
    /// Start a server with which clients will communicate.
    ///
    /// The server does not daemonize.
    Server(LaunchArgs),
    /// Run symbolication as a client to a previously started server.
    Client(SymbolicationArgs),
    /// Run symbolication as one-shot processing without using a server.
    Standalone {
        #[command(flatten)]
        launch_args: LaunchArgs,
        #[command(flatten)]
        symbolication_args: SymbolicationArgs,
    },
}

#[derive(Debug, Args)]
struct LaunchArgs {
    /// The path to the config file. If unspecified, uses `config.toml` (if it exists).
    #[arg(long, value_parser = existing_path)]
    config_file: Option<PathBuf>,
}

impl LaunchArgs {
    pub fn config_file(&self) -> &Path {
        self.config_file
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_CONFIG_FILE))
    }
}

#[derive(Debug, Args)]
struct SymbolicationArgs {
    /// The path from which to read input. If unspecified, input is read from stdin.
    #[arg(short, long)]
    input_file: Option<PathBuf>,

    /// The path to which to write the output. If unspecified, output is written to stdout.
    #[arg(short, long)]
    output_file: Option<PathBuf>,
}

/// Symbolicate StackTraces from crash annotations, and output a format compatible with signature
/// generation.
#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn run_with_symbolicator<F: AsyncFnOnce(&Arc<SymbolicateStacks>)>(
    config_file: &Path,
    f: F,
) -> anyhow::Result<()> {
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
        let maintenance_task = {
            let symbolicator = symbolicator.clone();
            tokio::spawn(async move { symbolicator.maintenance().await })
        };

        f(&symbolicator).await;

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

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .parse_default_env()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Server(args) => {
            if let Some(pid) = server::exists() {
                log::error!(
                    "only one server can run at a time (pid {} already running)",
                    pid
                );
                std::process::exit(2);
            }

            if let Err(e) = server::acquire_pid_lock() {
                log::error!("{e:#}");
                std::process::exit(2);
            }

            if let Err(e) = run_with_symbolicator(args.config_file(), server::run) {
                log::error!("failed to start server: {e:#}");
                std::process::exit(1);
            }
        }
        Command::Client(args) => {
            let mut input = args
                .input_file
                .map(|path| std::fs::File::open(path).map(|s| Box::new(s) as Box<dyn Read>))
                .unwrap_or_else(|| Ok(Box::new(std::io::stdin()) as Box<dyn Read>))
                .expect("failed to open input stream");

            let mut output = args
                .output_file
                .map(|path| std::fs::File::create(path).map(|f| Box::new(f) as Box<dyn Write>))
                .unwrap_or_else(|| Ok(Box::new(std::io::stdout())))
                .expect("failed to open output stream");

            let mut conn = server::connect().expect("failed to connect to server");

            std::io::copy(&mut input, &mut conn).expect("failed to write input to server");
            conn.write_all(&[0])
                .expect("failed to write input to server");

            let mut status = [0u8; 1];
            conn.read_exact(&mut status)
                .expect("failed to read from server");

            match status[0] {
                b'o' => std::io::copy(&mut conn, &mut output).expect("failed to read from server"),
                b'e' => {
                    std::io::copy(&mut conn, &mut std::io::stderr())
                        .expect("failed to read from server");
                    std::process::exit(1);
                }
                _ => unreachable!(),
            };
        }
        Command::Standalone {
            launch_args,
            symbolication_args,
        } => {
            let input = symbolication_args
                .input_file
                .map(|path| std::fs::read_to_string(path))
                .unwrap_or_else(|| {
                    let mut s = String::new();
                    std::io::stdin().read_to_string(&mut s)?;
                    Ok(s)
                })
                .expect("failed to read input");

            let mut output = symbolication_args
                .output_file
                .map(|path| std::fs::File::create(path).map(|f| Box::new(f) as Box<dyn Write>))
                .unwrap_or_else(|| Ok(Box::new(std::io::stdout())))
                .expect("failed to open output stream");

            run_with_symbolicator(launch_args.config_file(), async move |symbolicator| {
                match symbolicator.symbolicate(input).await {
                    Ok(s) => write!(output, "{}", s).expect("failed to write output"),
                    Err(e) => {
                        write!(std::io::stderr(), "{}", e).expect("failed to write to stderr");
                        std::process::exit(1);
                    }
                }
            })
            .expect("failed to start symbolicator");
        }
    }
}
