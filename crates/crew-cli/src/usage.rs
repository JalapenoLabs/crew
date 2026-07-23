//! `crew usage`: the shared-subscription usage gauge (issue #56).
//!
//! Reads the broker's one shared gauge (`GET /usage`) and prints it: the latest reading,
//! the auto-pause threshold, and whether new work is paused (and until when). The crew
//! shares one subscription, so the gauge is crew-wide. When usage crosses the threshold the
//! broker pauses new work until the window resets; `crew resume` lifts the pause early. The
//! broker address comes from `--broker`, else the `CREW_BROKER_*` environment.

use eyre::{eyre, Result, WrapErr};
use serde_json::Value;

use crate::broker::resolve_base;

/// Prints the shared-subscription usage gauge: the reading, the threshold, and any pause.
///
/// # Errors
/// Returns an error if the broker configuration is invalid, or the broker cannot be reached
/// or returns a malformed gauge.
pub fn usage(broker: Option<&str>) -> Result<()> {
    let base = resolve_base(broker)?;
    let url = format!("{base}/usage");
    let text = ureq::get(&url)
        .call()
        .map_err(|err| eyre!("could not reach the broker at {base}; is `crewd` running? ({err})"))?
        .into_string()
        .wrap_err("could not read the usage gauge")?;
    let gauge: Value = serde_json::from_str(&text).wrap_err("the usage gauge was malformed")?;
    println!("{}", render(&gauge));
    Ok(())
}

/// Renders the usage gauge as a one-line status for the operator.
fn render(gauge: &Value) -> String {
    let percent = gauge["percent"].as_u64().unwrap_or(0);
    let threshold = gauge["threshold"].as_u64().unwrap_or(0);
    if gauge["paused"].as_bool().unwrap_or(false) {
        let resets = gauge["resets_at"].as_str().unwrap_or("the window reset");
        format!(
            "Subscription usage {percent}% (auto-pause at {threshold}%): new work is paused \
             until {resets}. Resume early with `crew resume`."
        )
    } else {
        format!("Subscription usage {percent}% (auto-pause at {threshold}%): work is running.")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render;

    #[test]
    fn a_paused_gauge_names_the_reset_and_the_escape_hatch() {
        let line = render(&json!({
            "percent": 95, "threshold": 90, "paused": true,
            "resets_at": "2026-07-23T14:00:00Z",
        }));
        assert!(line.contains("95%") && line.contains("auto-pause at 90%"));
        assert!(line.contains("paused until 2026-07-23T14:00:00Z"));
        assert!(
            line.contains("crew resume"),
            "names the escape hatch: {line}"
        );
    }

    #[test]
    fn a_running_gauge_reads_as_running() {
        let line = render(&json!({ "percent": 40, "threshold": 90, "paused": false }));
        assert!(line.contains("40%") && line.contains("running"));
    }
}
