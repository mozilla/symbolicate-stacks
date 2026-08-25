//! Signature generation functionality.
//!
//! This currently requires an additional python program to perform signature generation. We could
//! embed a python interpreter here, but because of the GIL it won't be much more convenient than
//! calling externally.

use crate::symbolicator::Symbolicated;
use crate::Annotations;
use std::borrow::Cow;

pub fn create_input<'a>(
    symbolicated: &'a Option<Symbolicated>,
    annotations: &'a Annotations,
) -> Input<'a> {
    let java_exception = annotations
        .java_exception
        .as_deref()
        .and_then(JavaException::from_json);

    Input {
        symbolicated,
        java_exception,
        oom_allocation_size: annotations.oom_size,
        ipc_channel_error: annotations.ipc_channel_error.as_deref(),
        moz_crash_reason: annotations.moz_crash_reason.as_deref(),
        os: annotations.os.as_deref(),
        async_shutdown_timeout: annotations
            .async_shutdown_timeout
            .as_deref()
            .map(fixup_async_shutdown_timeout),
        hang_type: annotations
            .hang
            .as_deref()
            .map(|s| if s == "ui" { 1 } else { -1 }),
    }
}

// TODO abort_message (should be ping-scope rather than report-scope?)
#[derive(Debug, serde::Serialize)]
pub struct Input<'a> {
    #[serde(flatten)]
    symbolicated: &'a Option<Symbolicated>,
    java_exception: Option<JavaException<'a>>,
    oom_allocation_size: Option<u64>,
    ipc_channel_error: Option<&'a str>,
    moz_crash_reason: Option<&'a str>,
    os: Option<&'a str>,
    async_shutdown_timeout: Option<Cow<'a, str>>,
    hang_type: Option<i32>,
}

// AsyncShutdownTimeout has the `conditions` value serialized as a string, but it should be an
// array.
fn fixup_async_shutdown_timeout(s: &str) -> Cow<'_, str> {
    (|| {
        let mut value = serde_json::from_str::<serde_json::Value>(s).ok()?;
        let conditions = value.get_mut("conditions")?;
        let s = conditions.as_str()?;
        *conditions = serde_json::from_str::<serde_json::Value>(s).ok()?;
        Some(value.to_string())
    })()
    .map(Cow::Owned)
    .unwrap_or(Cow::Borrowed(s))
}

#[derive(Debug, serde::Serialize)]
struct JavaException<'a> {
    exception: Exception<'a>,
}

impl<'a> JavaException<'a> {
    fn from_json(json: &'a str) -> Option<Self> {
        let exception = match serde_json::from_str::<Option<json::JavaException>>(json) {
            Err(e) => {
                log::error!("failed to deserialize java exception: {e}");
                None
            }
            Ok(v) => v,
        }?;

        let mut module: Option<String> = None;
        let mut exc_type: Option<String> = None;

        let mut parse_exception = |s: &str| {
            let mut iter = s.rsplitn(2, ".");
            exc_type = iter.next().map(ToOwned::to_owned);
            module = iter.next().map(ToOwned::to_owned);
        };

        let frames;
        match exception {
            json::JavaException::V0 { messages, stack } => {
                // Try to get exception name from the messages.
                let first_message = messages.into_iter().nth(0);
                if let Some((front, _)) = first_message.as_ref().and_then(|m| m.split_once(":")) {
                    if front.ends_with("Exception") {
                        parse_exception(front);
                    }
                }
                frames = stack;
            }
            json::JavaException::V1 { throwables } => {
                if let Some(first) = throwables.into_iter().nth(0) {
                    parse_exception(&first.type_name);
                    frames = first.stack;
                } else {
                    frames = Default::default();
                }
            }
        }

        Some(JavaException {
            exception: Exception {
                values: vec![ExceptionValue::Stacktrace {
                    module,
                    exc_type,
                    frames,
                }],
            },
        })
    }
}

#[derive(Debug, serde::Serialize)]
struct Exception<'a> {
    values: Vec<ExceptionValue<'a>>,
}

#[derive(Debug, serde::Serialize)]
enum ExceptionValue<'a> {
    #[serde(rename = "stacktrace")]
    Stacktrace {
        module: Option<String>,
        #[serde(rename = "type")]
        exc_type: Option<String>,
        frames: Vec<ExceptionFrame<'a>>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ExceptionFrame<'a> {
    #[serde(rename(deserialize = "className"))]
    module: Option<Cow<'a, str>>,
    #[serde(rename(deserialize = "methodName"))]
    function: Option<Cow<'a, str>>,
    #[serde(rename(deserialize = "file"))]
    filename: Option<Cow<'a, str>>,
}

mod json {
    use serde::Deserialize;
    use std::borrow::Cow;

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    pub enum JavaException<'a> {
        // Order V1 before V0 because the keys of V0 are both optional so it will match a V1
        // serialization.
        V1 {
            #[serde(borrow)]
            throwables: Vec<Throwable<'a>>,
        },
        V0 {
            #[serde(default, borrow)]
            stack: Vec<super::ExceptionFrame<'a>>,
            #[serde(default, borrow)]
            messages: Vec<Cow<'a, str>>,
        },
    }

    #[derive(Debug, Deserialize)]
    pub struct Throwable<'a> {
        #[serde(rename(deserialize = "typeName"))]
        pub type_name: Cow<'a, str>,
        #[serde(default, borrow)]
        pub stack: Vec<super::ExceptionFrame<'a>>,
    }
}
