use anyhow::Context;
pub use config::Config;
pub use status::Status;
use std::sync::Arc;
use symbolicator::Symbolicator;

pub mod config;
mod signature;
pub mod status;
mod symbolicator;

const APP_USER_AGENT: &str = "symbolicate-stacks/1.0";
const ONLY_SYMBOLICATE_CRASHING_THREAD: bool = true;

pub struct SymbolicateStacks {
    pub status: Arc<Status>,
    symbolicator: Symbolicator,
}

impl SymbolicateStacks {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let status = Arc::new(Status::new(&config));
        let symbolicator = Symbolicator::new(&config, &status, ONLY_SYMBOLICATE_CRASHING_THREAD)?;
        Ok(Self {
            status,
            symbolicator,
        })
    }

    fn error_handler<T, E: std::fmt::Display>(result: Result<T, E>) -> Result<Option<T>, E> {
        match result {
            Err(e) => {
                log::error!("{e:#}");
                Ok(None)
            }
            Ok(v) => Ok(Some(v)),
        }
    }

    pub async fn symbolicate(&self, input: String) -> anyhow::Result<String> {
        let mut input: Annotations =
            serde_json::from_str(&input).context("while deserializing input")?;
        self.status.pings.inc_total();
        let symbolicated_frames = input.stack_traces.take().and_then(|stack_traces| {
            // Rather than adding an `Option` to the already complex
            // `symbolicate` method, just check for a "null" value here.
            (stack_traces != "null").then(|| {
                self.symbolicator
                    .symbolicate(stack_traces, input.channel.as_deref().unwrap_or("unknown"))
            })
        });
        if symbolicated_frames.is_some() {
            // Increment the status here to better represent the work to be
            // done. If done in the task, it may not run for a long while.
            self.status.pings.inc_symbolicating();
        }
        let status = self.status.clone();

        let symbolicated = if let Some(fut) = symbolicated_frames {
            let result = fut.await;
            status.pings.dec_symbolicating();
            Self::error_handler(result.context("failed to get symbolicated frames"))?
        } else {
            None
        };

        let input = signature::create_input(&symbolicated, &input);
        let input = serde_json::to_string(&input)?;
        status.pings.inc_complete();
        Ok(input)
    }

    pub async fn maintenance(&self) {
        self.symbolicator.maintenance().await
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Annotations {
    #[serde(rename = "StackTraces")]
    pub stack_traces: Option<String>,
    #[serde(rename = "JavaException")]
    pub java_exception: Option<String>,
    #[serde(rename = "MozCrashReason")]
    pub moz_crash_reason: Option<String>,
    pub ipc_channel_error: Option<String>,
    #[serde(rename = "OOMAllocationSize")]
    pub oom_size: Option<u64>,
    #[serde(rename = "OS")]
    pub os: Option<String>,
    #[serde(rename = "ReleaseChannel")]
    pub channel: Option<String>,
    #[serde(rename = "AsyncShutdownTimeout")]
    pub async_shutdown_timeout: Option<String>,
    #[serde(rename = "Hang")]
    pub hang: Option<String>,
}
