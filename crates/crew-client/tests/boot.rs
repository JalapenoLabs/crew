//! End-to-end test of booting a role from its card (issue #18).
//!
//! It starts a `crewd` instance in-process on an ephemeral loopback port, then
//! boots a role the way the `crew-mcp` binary does: parse a role card with the
//! shared loader, build the broker client from the card's address, and register
//! the role on the roster. The assertions prove the acceptance: a role boots
//! knowing its lane and reaches the broker with no extra prompt.
//!
//! The broker runs on a background thread with its own tokio runtime, so the
//! test body stays synchronous and can call the blocking `ureq`-based client
//! directly.

mod common;

use common::start_broker;
use crew_client::Broker;
use crew_core::RoleCard;

#[test]
fn a_role_boots_from_its_card_and_reaches_the_broker() {
    let addr = start_broker();

    // The card is all the agent is handed: its role, its lane, its acceptance bar,
    // and the broker address. Nothing else is needed to boot.
    let card_toml = format!(
        "role = \"backend\"\n\
         owned_paths = [\"api/\", \"db/\"]\n\
         acceptance = \"Tests green, migrations reversible.\"\n\n\
         [broker]\n\
         host = \"{}\"\n\
         port = {}\n",
        addr.ip(),
        addr.port(),
    );

    // Boot exactly as the binary does: parse the card with the shared loader, then
    // build the broker client straight from the card's address.
    let card = RoleCard::from_toml(&card_toml).expect("the card parses");
    let broker = Broker::new(
        card.broker.base_url(),
        card.role.clone(),
        card.commander.clone(),
    );

    // The briefing proves the role boots knowing its lane and where the unit is.
    let briefing = card.briefing();
    assert!(
        briefing.contains("api/, db/"),
        "the briefing states the lane"
    );
    assert!(
        briefing.contains(&card.broker.base_url()),
        "the briefing gives the broker address",
    );

    // Reaching the broker: register the role and its lane on the roster.
    broker
        .register(&card.owned_paths)
        .expect("the role registers on the roster");

    // The unit now sees the role, owning exactly the lane its card declared.
    let roster = broker.roster().expect("the roster is readable");
    let entry = roster
        .roles
        .iter()
        .find(|entry| entry.role == "backend")
        .expect("the booted role appears on the roster");
    assert_eq!(
        entry.owned_paths,
        ["api/", "db/"],
        "it owns its declared lane"
    );
    assert_eq!(
        entry.liveness, "working",
        "a freshly booted role is working"
    );
}
