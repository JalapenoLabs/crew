//! Resolving the broker base URL and reading broker errors, shared by every
//! operator command (issue #186).
//!
//! Both the stream readers (`crew watch`, `crew notify`, `crew top`, `crew
//! usage`) and the request/response commands (`crew pause` / `crew resume` /
//! `crew standdown`, `crew brief` / `crew redirect` / `crew belay` / `crew
//! command` / `crew reassign`) resolve `--broker` (or the `CREW_BROKER_*`
//! environment) and read a typed broker error the same way. These helpers put
//! that resolution and error format in one place, so every command agrees.

use crew_substrate::{broker::Config as BrokerConfig, core::BrokerEndpoint};
use eyre::{Result, WrapErr};

/// The broker base URL: the `--broker` value if given, else the broker's
/// environment.
///
/// A `--broker` value is normalized (the scheme defaults to `http`, a trailing
/// slash is dropped); with none, the address comes from the `CREW_BROKER_*`
/// environment the broker itself reads.
///
/// # Errors
/// Returns an error if `CREW_BROKER_HOST` or `CREW_BROKER_PORT` is set but
/// invalid.
pub(crate) fn resolve_base(flag: Option<&str>) -> Result<String> {
    if let Some(url) = flag {
        return Ok(normalize_base(url));
    }
    let config = BrokerConfig::from_env().wrap_err("could not read the broker configuration")?;
    Ok(BrokerEndpoint::new(config.host.to_string(), config.port).base_url())
}

/// Normalizes a `--broker` value: default the scheme to `http`, drop a trailing
/// slash.
fn normalize_base(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_owned()
    } else {
        format!("http://{url}")
    }
}

/// The `{ "error": ... }` message from a broker error response, if any.
///
/// The broker renders a typed 4xx/5xx as `{"error":"..."}`, so a command shows
/// the broker's reason rather than a bare status code.
pub(crate) fn broker_error(response: ureq::Response) -> Option<String> {
    let text = response.into_string().ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get("error")?.as_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::normalize_base;

    #[test]
    fn normalize_base_defaults_the_scheme_and_trims() {
        assert_eq!(normalize_base("localhost:2739/"), "http://localhost:2739");
        assert_eq!(
            normalize_base("http://127.0.0.1:2739"),
            "http://127.0.0.1:2739"
        );
        assert_eq!(
            normalize_base("https://broker.internal"),
            "https://broker.internal"
        );
    }
}
