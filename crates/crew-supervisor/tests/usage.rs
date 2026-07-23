//! Subscription usage auto-pause, end to end against a real broker (issue #56).
//!
//! Proves the acceptance: a crossed usage threshold pauses new work (gauge and roster show
//! it), and the operator can resume early. The reset auto-clear and the threshold logic are
//! unit-tested in the broker; this proves the supervisor's `report_usage` seam reaches the
//! broker and engages the one shared gauge. The usage signal is the rate-limit detection of
//! the stream-json parser (issue #24); this drives the seam directly.

mod common;

use common::start_broker;
use crew_core::Timestamp;
use crew_supervisor::RosterClient;

/// A window reset far in the future, so an engaged pause holds through the test.
fn future_reset() -> Timestamp {
    serde_json::from_value(serde_json::json!("2099-01-01T00:00:00Z")).unwrap()
}

/// The `GET /usage` gauge.
fn usage(base: &str) -> serde_json::Value {
    let text = ureq::get(&format!("{base}/usage"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// Whether the roster reports the crew usage-paused.
fn roster_usage_paused(base: &str) -> bool {
    let text = ureq::get(&format!("{base}/roster"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["usage_paused"].as_bool().unwrap()
}

#[test]
fn a_reported_reading_over_the_threshold_auto_pauses_the_crew_and_resumes() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());

    // Under the default 90 percent threshold: no pause.
    roster.report_usage(70, future_reset()).unwrap();
    assert_eq!(usage(&base)["paused"], false);
    assert!(!roster_usage_paused(&base));

    // A reading over the threshold auto-pauses new work across the whole crew.
    roster.report_usage(96, future_reset()).unwrap();
    let gauge = usage(&base);
    assert_eq!(gauge["percent"], 96);
    assert_eq!(gauge["paused"], true, "over the threshold: {gauge}");
    assert!(
        roster_usage_paused(&base),
        "the roster surfaces the auto-pause so every role honors it"
    );

    // The operator resumes early with `crew resume` (POST /resume, crew-wide).
    ureq::post(&format!("{base}/resume"))
        .set("content-type", "application/json")
        .send_string("{}")
        .unwrap();
    assert_eq!(usage(&base)["paused"], false, "resumed early");
    assert!(!roster_usage_paused(&base));
}
