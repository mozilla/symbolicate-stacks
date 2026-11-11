use anyhow::Context;
use clap::{Args, Parser};
use lib::{Config, SymbolicateStacks, config::WorkerThreads};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
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

fn byte_size(s: &str) -> Result<u64, std::num::ParseIntError> {
    const SUFFIXES: [(&str, u32); 4] = [("k", 3), ("M", 6), ("G", 9), ("T", 12)];
    for (suffix, pow) in SUFFIXES {
        if let Some(n) = s.strip_suffix(suffix) {
            return Ok(n.parse::<u64>()? * 10u64.pow(pow));
        }
    }

    s.parse::<u64>()
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
    #[arg(short, long, conflicts_with = "clean_cache")]
    input_file: Option<PathBuf>,

    /// The path to which to write the output. If unspecified, output is written to stdout.
    #[arg(short, long, conflicts_with = "clean_cache")]
    output_file: Option<PathBuf>,
}

/// Symbolicate StackTraces from crash annotations, and output a format compatible with signature
/// generation.
#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    launch_args: LaunchArgs,

    /// Only clean the cache, evicting entries in order of least recent modification until below the
    /// given size threshold.
    ///
    /// The given value can optionally have one of the following suffixes: k, M, G, T. Without a
    /// value, the limit is read from the config file.
    #[arg(long, value_name = "BYTES", value_parser = byte_size)]
    clean_cache: Option<Option<u64>>,

    #[command(flatten)]
    symbolication_args: SymbolicationArgs,
}

fn read_config_file(path: &Path) -> anyhow::Result<Config> {
    std::fs::read_to_string(path)
        .map_err(|e| anyhow::Error::from(e))
        .and_then(|s| Ok(toml::from_str(&s)?))
        .with_context(|| format!("while reading {}", path.display()))
}

fn tokio_runtime(config: &Config) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("symbolicate-stacks");

    if let WorkerThreads::Exact(n) = config.worker_threads {
        builder.worker_threads(n);
    }

    builder.build()
}

fn run_with_symbolicator<F: AsyncFnOnce(&Arc<SymbolicateStacks>)>(
    config_file: &Path,
    f: F,
) -> anyhow::Result<()> {
    let config: Config = read_config_file(config_file)?;
    let runtime = tokio_runtime(&config)?;
    let symbolicator = Arc::new(SymbolicateStacks::new(config)?);

    runtime.block_on(async move {
        f(&symbolicator).await;
    });

    Ok(())
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .parse_default_env()
        .init();

    let cli = Cli::parse();

    let Cli {
        launch_args,
        clean_cache,
        symbolication_args,
    } = cli;

    if let Some(limit) = clean_cache {
        let config =
            read_config_file(launch_args.config_file()).expect("failed to read config file");
        let runtime = tokio_runtime(&config).expect("failed to create runtime");
        let limit = limit
            .or(config.cache.fs_size_limit_gb.limit_bytes())
            .expect("no cache limit set");
        runtime
            .block_on(lib::clean_cache(&config, limit))
            .expect("failed to clean cache");
        std::process::exit(0);
    }

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

    run_with_symbolicator(
        launch_args.config_file(),
        async move |symbolicator| match symbolicator.symbolicate(input).await {
            Ok(s) => write!(output, "{s}").expect("failed to write output"),
            Err(e) => {
                write!(std::io::stderr(), "{e:#}").expect("failed to write to stderr");
                std::process::exit(1);
            }
        },
    )
    .expect("failed to start symbolicator");
}
