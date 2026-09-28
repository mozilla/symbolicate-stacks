//! HTTP helpers built around reqwest.

mod retry {
    const MAX_ATTEMPTS: u32 = 6;

    const RETRY_STATUS_CODES: &[reqwest::StatusCode] = &[
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        reqwest::StatusCode::BAD_GATEWAY,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        reqwest::StatusCode::GATEWAY_TIMEOUT,
    ];

    /// Retry a request based on the status code.
    ///
    /// This will retry up to 5 times (6 total requests) in the following cases:
    /// - 429 Too Many Requests
    /// - 500 Internal Server Error
    /// - 502 Bad Gateway
    /// - 503 Service Unavailable
    /// - 504 Gateway Timeout
    ///
    /// Retries will have a backoff up to 8 seconds long (scaling with number of attempts).
    pub async fn retry_requests(
        builder: reqwest::RequestBuilder,
    ) -> reqwest::Result<reqwest::Response> {
        let mut attempts: u32 = 0;
        loop {
            attempts += 1;
            log::debug!("requesting {builder:?}");
            let response = builder.try_clone().unwrap().send().await?;
            let status = response.status();
            if !(status.is_client_error() || status.is_server_error()) {
                log::trace!("request success response");
                return Ok(response);
            } else {
                log::trace!("request error: {status}");
                if RETRY_STATUS_CODES.contains(&status) && attempts < MAX_ATTEMPTS {
                    log::debug!("received status code {status}; retrying (attempt {attempts})");
                    let to_sleep = retry_after_remaining(&response).unwrap_or_else(|| {
                        std::time::Duration::from_millis(250 * 2u64.pow(attempts))
                    });
                    tokio::time::sleep(to_sleep).await;
                    continue;
                }
                return response.error_for_status();
            }
        }
    }

    /// Best-effort parsing of the Retry-After header, returning the remaining duration.
    fn retry_after_remaining(response: &reqwest::Response) -> Option<std::time::Duration> {
        let header = response.headers().get(reqwest::header::RETRY_AFTER)?;
        let header_str = header.to_str().ok()?;
        if let Ok(delay_seconds) = header_str.parse::<u64>() {
            Some(std::time::Duration::from_secs(delay_seconds))
        } else if let Ok(time) = httpdate::parse_http_date(header_str) {
            Some(
                time.duration_since(std::time::SystemTime::now())
                    .unwrap_or(std::time::Duration::ZERO),
            )
        } else {
            None
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum HeadError {
    #[error("{0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Location header with non-ASCII value")]
    InvalidLocation,
}

/// Perform a HEAD request (with retries), but always redirect if a 200 response has a `Location`
/// header.
///
/// Some servers, like the mozilla symbol server, respond with 200 to indicate an entry exists,
/// _without_ redirecting. They still provide the `Location` header.
pub async fn head_always_redirect(
    client: &reqwest::Client,
    url: &str,
) -> Result<reqwest::Response, HeadError> {
    let mut response = retry_requests(client.head(url)).await?;
    if let Some(location) = response.headers().get(reqwest::header::LOCATION) {
        let Ok(location_str) = location.to_str() else {
            return Err(HeadError::InvalidLocation);
        };

        response = retry_requests(client.head(location_str)).await?;
    }
    Ok(response)
}

mod download_size {
    const AMAZON_ORIGIN_LENGTH: reqwest::header::HeaderName =
        reqwest::header::HeaderName::from_static("x-amz-meta-original_size");

    const GOOGLE_CONTENT_LENGTH: reqwest::header::HeaderName =
        reqwest::header::HeaderName::from_static("x-goog-stored-content-length");

    pub async fn get(client: &reqwest::Client, url: &str) -> Result<Option<u64>, super::HeadError> {
        let response = super::head_always_redirect(client, url).await?;

        fn int_header<K: reqwest::header::AsHeaderName>(
            response: &reqwest::Response,
            name: K,
        ) -> Option<u64> {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
        }

        let size_headers = [
            (!response
                .headers()
                .contains_key(reqwest::header::CONTENT_ENCODING))
            .then_some(reqwest::header::CONTENT_LENGTH),
            Some(AMAZON_ORIGIN_LENGTH),
            Some(GOOGLE_CONTENT_LENGTH),
        ];

        Ok(size_headers
            .into_iter()
            .find_map(|h| h.and_then(|h| int_header(&response, h))))
    }
}

pub use download_size::get as get_download_size;
pub use retry::retry_requests;

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn location_200() {
        let mut s = mockito::Server::new_async().await;

        let redirect = s
            .mock("HEAD", "/redirect")
            .with_status(200)
            .with_header(reqwest::header::LOCATION, &format!("{}/target", s.url()))
            .create_async()
            .await;

        let target = s.mock("HEAD", "/target").create_async().await;

        let client = reqwest::Client::new();

        head_always_redirect(&client, &format!("{}/redirect", s.url()))
            .await
            .expect("head request failed");

        redirect.assert_async().await;
        target.assert_async().await;
    }

    #[tokio::test]
    async fn file_size() {
        let mut s = mockito::Server::new_async().await;

        let file = s
            .mock("HEAD", "/file")
            .with_status(200)
            .with_header(reqwest::header::CONTENT_LENGTH, "42")
            .create_async()
            .await;

        let client = reqwest::Client::new();

        let size = get_download_size(&client, &format!("{}/file", s.url()))
            .await
            .expect("request failed");

        assert_eq!(size, Some(42));

        file.assert_async().await;
    }

    #[tokio::test]
    async fn retry_codes() {
        use std::{collections::HashMap, sync::Mutex};

        const RETRY_CODES: [u16; 5] = [429, 500, 502, 503, 504];

        let mut s = mockito::Server::new_async().await;
        let requests: Mutex<HashMap<usize, usize>> = Default::default();

        let m = s
            .mock("GET", mockito::Matcher::Any)
            .with_status_code_from_request(move |request| {
                let code = request.path().strip_prefix("/errorfirst/").unwrap();
                let code = code.parse::<usize>().unwrap();

                let mut guard = requests.lock().unwrap();
                let count = guard.entry(code).or_default();
                *count += 1;
                if *count == 1 { code } else { 200 }
            })
            .expect(RETRY_CODES.len() * 2)
            .create_async()
            .await;

        let client = reqwest::Client::new();

        for code in RETRY_CODES {
            let builder = client.get(format!("{}/errorfirst/{}", s.url(), code));
            retry_requests(builder)
                .await
                .expect("request failed unexpectedly");
        }

        m.assert_async().await;
    }

    #[tokio::test]
    async fn retry_after_delay() {
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

        let mut s = mockito::Server::new_async().await;

        let requested: AtomicBool = false.into();
        let m = s
            .mock("GET", "/retry_after_delay")
            .with_header(reqwest::header::RETRY_AFTER, "1")
            .with_status_code_from_request(move |_| {
                if !requested.swap(true, Relaxed) {
                    429
                } else {
                    200
                }
            })
            .expect(2)
            .create_async()
            .await;

        let client = reqwest::Client::new();

        let builder = client.get(format!("{}/retry_after_delay", s.url()));

        let start = std::time::Instant::now();
        retry_requests(builder)
            .await
            .expect("request failed unexpectedly");
        assert!(start.elapsed() >= std::time::Duration::from_secs(1));

        m.assert_async().await;
    }

    fn round_up_seconds(s: std::time::SystemTime) -> std::time::SystemTime {
        let duration = s.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
        if duration.subsec_nanos() > 0 {
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::new(duration.as_secs() + 1, 0)
        } else {
            s
        }
    }

    #[tokio::test]
    async fn retry_after_http_date() {
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

        let mut s = mockito::Server::new_async().await;

        // We need to round up the seconds because HTTP dates only have second resolution, and we
        // will use this SystemTime later to verify that we are beyond that time.
        let later =
            round_up_seconds(std::time::SystemTime::now() + std::time::Duration::from_secs(1));
        let requested: AtomicBool = false.into();
        let m = s
            .mock("GET", "/retry_after_http_date")
            .with_header(
                reqwest::header::RETRY_AFTER,
                &httpdate::fmt_http_date(later),
            )
            .with_status_code_from_request(move |_| {
                if !requested.swap(true, Relaxed) {
                    429
                } else {
                    200
                }
            })
            .expect(2)
            .create_async()
            .await;

        let client = reqwest::Client::new();

        let builder = client.get(format!("{}/retry_after_http_date", s.url()));

        retry_requests(builder)
            .await
            .expect("request failed unexpectedly");
        assert!(later.elapsed().is_ok());

        m.assert_async().await;
    }
}
