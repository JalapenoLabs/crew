//! End-to-end test of the spawn-time briefing fetch (issue #122).
//!
//! The supervisor folds a role's briefing packet into its opening `claude -p`
//! turn at spawn, so bounded context is in context even if the agent never
//! calls `crew_briefing`. This proves the fetch path (`RosterClient::briefing`)
//! against a real broker: a decision recorded on the situation board shows up
//! in the packet, and the packet always carries its header, so the boot
//! injection has real, current content to fold in.

mod common;

use common::start_broker;
use crew_core::RoleId;
use crew_supervisor::RosterClient;
use serde_json::json;

/// Records a board entry through the broker.
fn record_board(base: &str, key: &str, section: &str, body: &str) {
    ureq::post(&format!("{base}/board"))
        .set("content-type", "application/json")
        .send_string(
            &json!({ "role": "commander", "key": key, "section": section, "body": body })
                .to_string(),
        )
        .unwrap();
}

#[test]
fn the_briefing_packet_is_fetchable_and_reflects_the_board() {
    let base = start_broker();

    // The crew's durable memory a fresh role needs: a recorded decision.
    record_board(
        &base,
        "auth-strategy",
        "decision",
        "JWT with 15m access tokens",
    );

    let roster = RosterClient::new(base);
    let packet = roster.briefing(&RoleId::new("backend")).unwrap();

    assert!(
        packet.contains("Briefing for backend"),
        "the packet carries its header, so the boot prompt always gains content: {packet}"
    );
    assert!(
        packet.contains("auth-strategy") && packet.contains("JWT with 15m access tokens"),
        "the packet reflects the live board decision: {packet}"
    );
}

#[test]
fn a_missing_broker_makes_the_fetch_fail_rather_than_hang() {
    // The spawn treats this as "no packet" and boots on the card briefing; here we
    // just prove the fetch surfaces an error fast rather than hanging the spawn.
    let roster = RosterClient::new("http://127.0.0.1:1");
    assert!(
        roster.briefing(&RoleId::new("backend")).is_err(),
        "an unreachable broker is an error the caller degrades from, not a hang"
    );
}
