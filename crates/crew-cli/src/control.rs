//! The General's operator-facing sends: the free-form `crew brief`, the
//! steering directives `crew redirect`, `crew belay`, and `crew command`, and
//! the task reassignment `crew reassign`.
//!
//! `crew brief` is the General's plain send (issue #118): a free-form `note` to
//! the commander by default, a named role, or a channel (`all-units` or a
//! pair). It is the counterpart to the agent shim's `crew send`
//! (`src/shim.rs`), but posts as the General rather than as an agent's role, so
//! it is how the General sets the unit to work or broadcasts to all-units.
//!
//! The directives let the General steer a running agent without tearing the
//! crew down (issue #38). Each posts a high-priority message to a role's direct
//! channel, from the General; the broker delivers it on the role's
//! self-filtered inbox stream (issue #10), and the role honors it at its next
//! tool boundary (its briefing tells it so). A `redirect` steers a role without
//! stopping it; a `belay` halts its current work and re-tasks it. Delivery is
//! the same whether the role is mid-turn or idle: the message lands on the
//! inbox, never by killing the process.
//!
//! `crew command` is the **direct override** (issue #42): the General orders a
//! specialist itself, bypassing the commander's fan-out, and the commander is
//! informed rather than bypassed silently, so the chain of command stays
//! intact. The default (brief the commander) is unchanged; the override is
//! explicit. `crew reassign` is its other half: the General moves an in-flight
//! task from one role to another in the work ledger, notifying both roles and
//! the commander, so work in progress changes hands cleanly (issue #42).
//!
//! All post as the General, so unlike the agent shim they need no role card:
//! the broker address comes from `--broker`, else the `CREW_BROKER_*`
//! environment.

use crew_substrate::{
    broker::Config as BrokerConfig,
    core::{BrokerEndpoint, Channel, RoleId},
};
use eyre::{eyre, Result, WrapErr};
use serde_json::json;

/// Briefs the crew as the General: post a free-form `note` to the commander by
/// default, a named role, or a channel (issue #118).
///
/// This is the General's plain send, distinct from the agent shim's `crew send`
/// (which posts as an agent's role and needs a role card): it posts as the
/// General, so it needs only the broker address. The target follows the crew's
/// one addressing rule ([`Channel::resolve`]): `to` a role wins, else `channel`
/// a name (`all-units` or a pair like `a+b`), else the commander. This is both
/// the default brief that sets the unit to work and the all-units broadcast.
///
/// # Errors
/// Returns an error if the target is not routable, the broker configuration is
/// invalid, or the broker cannot be reached or rejects the message.
pub fn brief(
    broker: Option<&str>,
    to: Option<&str>,
    channel: Option<&str>,
    commander: Option<&str>,
    body: &str,
) -> Result<()> {
    let target = brief_target(to, channel, commander)?;
    let base = resolve_base(broker)?;
    let payload = json!({ "from": { "kind": "general" }, "kind": "note", "body": body });
    post_message(&base, target.name().as_str(), &payload, "brief")?;
    println!("briefed {}", target.name());
    Ok(())
}

/// Resolves a brief's target: `to` a role, else `channel` a name, else the
/// commander, applying the crew's one addressing rule ([`Channel::resolve`]).
fn brief_target(
    to: Option<&str>,
    channel: Option<&str>,
    commander: Option<&str>,
) -> Result<Channel> {
    let commander = RoleId::new(default_commander(commander));
    Channel::resolve(to, channel, &commander).ok_or_else(|| {
        eyre!("that is not a routable target; name a role, `all-units`, or a pair like `a+b`")
    })
}

/// Steers `role` mid-task without stopping it: post the General's `redirect`.
///
/// # Errors
/// Returns an error if `role` is not a plain role name, the broker
/// configuration is invalid, or the broker cannot be reached or rejects the
/// message.
pub fn redirect(broker: Option<&str>, role: &str, message: &str) -> Result<()> {
    direct(broker, role, "redirect", message)
}

/// Halts `role`'s current work and re-tasks it: post the General's `belay`.
///
/// # Errors
/// Returns an error if `role` is not a plain role name, the broker
/// configuration is invalid, or the broker cannot be reached or rejects the
/// message.
pub fn belay(broker: Option<&str>, role: &str, order: &str) -> Result<()> {
    direct(broker, role, "belay", order)
}

/// Commands `role` directly, bypassing the commander's fan-out, and informs the
/// commander so the override is visible rather than silent (issue #42).
///
/// The General overrides the commander to order a specialist itself: it posts
/// an `order` from the General to the role's `@role` channel (`title`, plus
/// `scope` and `acceptance` when given), and then a note to the commander's
/// feed announcing the direct order, so a reassignment or a direct task is
/// never bypassing the commander behind its back. Ordering the commander itself
/// carries no notice (it is the addressee). The default, briefing the
/// commander, is unchanged; this is the deliberate override.
///
/// # Errors
/// Returns an error if `role` (or the commander) is not a plain role name, the
/// broker configuration is invalid, or the broker cannot be reached or rejects
/// a message.
pub fn command(
    broker: Option<&str>,
    role: &str,
    order: &str,
    scope: Option<&str>,
    acceptance: Option<&str>,
    commander: Option<&str>,
) -> Result<()> {
    let role = plain_role(role);
    let target = role_channel(&role, "command")?;
    let commander = default_commander(commander);
    let base = resolve_base(broker)?;

    // The direct order to the specialist: the General taking the commander's
    // ordering role.
    let order_payload = json!({
        "from": { "kind": "general" },
        "kind": "order",
        "title": order,
        "scope": scope.unwrap_or_default(),
        "owned_paths": [],
        "acceptance": acceptance.unwrap_or_default(),
    });
    post_message(&base, target.name().as_str(), &order_payload, "order")?;

    // Inform the commander, unless it is the one being commanded.
    if role.eq_ignore_ascii_case(&commander) {
        println!("ordered {role} directly");
        return Ok(());
    }
    let commander_channel = role_channel(&commander, "inform")?;
    let notice = json!({
        "from": { "kind": "general" },
        "kind": "note",
        "body": commander_notice(&role, order),
    });
    post_message(
        &base,
        commander_channel.name().as_str(),
        &notice,
        "commander notice",
    )?;
    println!("ordered {role} directly; informed {commander}");
    Ok(())
}

/// Reassigns an in-flight task from its current owner to `to`, the General's
/// authoritative override, and notifies both roles and the commander (issue
/// #42).
///
/// This is the second half of the direct override: where [`command`] hands one
/// role a fresh order, this **moves work already in flight**. It POSTs to the
/// broker's `/ledger/reassign`, which moves the held task's owner (overriding
/// the ledger's one-owner invariant, preserving the task's state) and publishes
/// a `ledger` event, so the change is authoritative on the stream. It then
/// posts a note to each party so no one is surprised: the old owner is told to
/// hand the work off, the new owner to pick it up, and the commander that the
/// General moved it, unless the commander is one of the two roles (it is
/// already notified as a party). `from`, when given, guards a stale view: the
/// broker refuses the move if that role no longer holds the task.
///
/// # Errors
/// Returns an error if `to` (or `from`) is not a plain role name, the broker
/// configuration is invalid, the broker refuses the reassignment (the task is
/// not held, is held by a role other than `from`, or is already owned by `to`),
/// or a notification cannot be posted.
pub fn reassign(
    broker: Option<&str>,
    task: &str,
    to: &str,
    from: Option<&str>,
    commander: Option<&str>,
) -> Result<()> {
    let to = plain_role(to);
    let to_channel = role_channel(&to, "reassign to")?;
    let from = from.map(plain_role);
    let base = resolve_base(broker)?;

    // Move the ledger owner authoritatively; the broker returns who held it and
    // the state the task keeps, so the notes are precise.
    let outcome = post_reassign(&base, task, &to, from.as_deref())?;
    let previous_owner = outcome.previous_owner;

    // Notify the old owner to hand off, and the new owner to pick the work up.
    let old_channel = role_channel(&previous_owner, "notify")?;
    let old_note = general_note(&reassign_old_owner_notice(task, &to));
    post_message(
        &base,
        old_channel.name().as_str(),
        &old_note,
        "old-owner notice",
    )?;

    let new_note = general_note(&reassign_new_owner_notice(
        task,
        &previous_owner,
        &outcome.state,
    ));
    post_message(
        &base,
        to_channel.name().as_str(),
        &new_note,
        "new-owner notice",
    )?;

    // Inform the commander, unless it is one of the two parties (already notified).
    let commander = default_commander(commander);
    if commander.eq_ignore_ascii_case(&previous_owner) || commander.eq_ignore_ascii_case(&to) {
        println!("reassigned `{task}` from {previous_owner} to {to}");
        return Ok(());
    }
    let commander_channel = role_channel(&commander, "inform")?;
    let notice = general_note(&reassign_commander_notice(task, &previous_owner, &to));
    post_message(
        &base,
        commander_channel.name().as_str(),
        &notice,
        "commander notice",
    )?;
    println!("reassigned `{task}` from {previous_owner} to {to}; informed {commander}");
    Ok(())
}

/// What the broker reported for a reassignment: who held the task, and the
/// state it kept across the move.
struct ReassignOutcome {
    /// The role that held the task before the reassignment.
    previous_owner: String,
    /// The task's state (`claimed` / `in_progress` / `blocked`), for the new
    /// owner's notice.
    state: String,
}

/// POSTs a reassignment to the broker's `/ledger/reassign`, returning who held
/// the task and its preserved state, or surfacing the broker's refusal.
fn post_reassign(base: &str, task: &str, to: &str, from: Option<&str>) -> Result<ReassignOutcome> {
    let mut body = json!({ "task": task, "to": to });
    if let Some(from) = from {
        body["from"] = json!(from);
    }
    let url = format!("{base}/ledger/reassign");
    let response = match ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&body.to_string())
    {
        Ok(response) => response,
        // The broker answered with a typed 4xx/5xx (a stale view, or nothing to move).
        Err(ureq::Error::Status(code, response)) => {
            let reason = broker_error(response).unwrap_or_else(|| format!("HTTP {code}"));
            return Err(eyre!("the broker refused the reassignment: {reason}"));
        }
        // A transport error means the broker is unreachable.
        Err(err) => {
            return Err(err).wrap_err_with(|| {
                format!("could not reach the broker at {base}; is `crewd` running?")
            });
        }
    };
    let text = response
        .into_string()
        .wrap_err("could not read the reassignment response")?;
    let value: serde_json::Value =
        serde_json::from_str(&text).wrap_err("could not parse the reassignment response")?;
    let previous_owner = value["from"]
        .as_str()
        .ok_or_else(|| eyre!("the reassignment response omitted the previous owner"))?
        .to_owned();
    let state = value["state"].as_str().unwrap_or_default().to_owned();
    Ok(ReassignOutcome {
        previous_owner,
        state,
    })
}

/// A `note` payload from the General with `body`.
fn general_note(body: &str) -> serde_json::Value {
    json!({ "from": { "kind": "general" }, "kind": "note", "body": body })
}

/// The note telling the old owner its in-flight task has been reassigned away.
fn reassign_old_owner_notice(task: &str, new_owner: &str) -> String {
    format!(
        "The General reassigned `{task}` to {new_owner}. Hand it off cleanly; you no longer own \
         it."
    )
}

/// The note telling the new owner it now holds the reassigned task, and where
/// the work stands.
fn reassign_new_owner_notice(task: &str, previous_owner: &str, state: &str) -> String {
    let standing = if state.is_empty() {
        String::new()
    } else {
        format!(" (currently `{state}`)")
    };
    format!(
        "The General reassigned `{task}` to you, from {previous_owner}. Pick it up where it \
         stands{standing}."
    )
}

/// The note informing the commander of a reassignment, so the override is not
/// silent.
fn reassign_commander_notice(task: &str, previous_owner: &str, new_owner: &str) -> String {
    format!(
        "The General reassigned `{task}` from {previous_owner} to {new_owner}. You are informed, \
         not bypassed: adjust your plan around it."
    )
}

/// Posts a General directive (`kind`, a `redirect` or `belay`) to `role`'s
/// direct channel with `body`, printing a short confirmation.
fn direct(broker: Option<&str>, role: &str, kind: &str, body: &str) -> Result<()> {
    let role = plain_role(role);
    let target = role_channel(&role, "steer")?;
    let base = resolve_base(broker)?;
    let payload = json!({ "from": { "kind": "general" }, "kind": kind, "body": body });
    post_message(&base, target.name().as_str(), &payload, kind)?;
    println!("{kind} sent to {role}");
    Ok(())
}

/// Strips a leading `@` and surrounding whitespace from a role name.
fn plain_role(role: &str) -> String {
    role.trim().trim_start_matches('@').to_owned()
}

/// The commander to address by default: the given name if any, else the
/// conventional `commander`, with a leading `@` and whitespace stripped.
///
/// The General has no role card to name the crew's commander, so this matches
/// [`RoleCard`](crew_substrate::core::RoleCard)'s own default when a card omits
/// it.
fn default_commander(name: Option<&str>) -> String {
    name.map(str::trim)
        .filter(|name| !name.is_empty())
        .map_or_else(|| "commander".to_owned(), plain_role)
}

/// The direct `@role` channel for `role`, or an error naming what the caller
/// wanted to do.
fn role_channel(role: &str, verb: &str) -> Result<Channel> {
    Channel::parse(&format!("@{role}"))
        .filter(|channel| matches!(channel, Channel::Direct(_)))
        .ok_or_else(|| eyre!("`{role}` is not a role to {verb}; name a single specialist"))
}

/// The note that informs the commander of a direct order, so the override is
/// not silent.
fn commander_notice(role: &str, order: &str) -> String {
    format!(
        "Direct order from the General to {role}: {order}. You are informed, not bypassed: \
         adjust your plan around it."
    )
}

/// Posts `payload` to `channel`'s message endpoint, surfacing a broker refusal
/// as `what`.
fn post_message(base: &str, channel: &str, payload: &serde_json::Value, what: &str) -> Result<()> {
    let url = format!("{base}/channels/{channel}/messages");
    match ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
    {
        Ok(_) => Ok(()),
        // The broker answered with a typed 4xx/5xx; surface its reason.
        Err(ureq::Error::Status(code, response)) => {
            let reason = broker_error(response).unwrap_or_else(|| format!("HTTP {code}"));
            Err(eyre!("the broker rejected the {what}: {reason}"))
        }
        // A transport error means the broker is unreachable.
        Err(err) => Err(err)
            .wrap_err_with(|| format!("could not reach the broker at {base}; is `crewd` running?")),
    }
}

/// The broker base URL: the `--broker` value if given, else the broker's
/// environment.
///
/// # Errors
/// Returns an error if `CREW_BROKER_HOST` or `CREW_BROKER_PORT` is set but
/// invalid.
fn resolve_base(flag: Option<&str>) -> Result<String> {
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
fn broker_error(response: ureq::Response) -> Option<String> {
    let text = response.into_string().ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get("error")?.as_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::{
        brief_target, commander_notice, default_commander, normalize_base, plain_role,
        reassign_commander_notice, reassign_new_owner_notice, reassign_old_owner_notice,
        role_channel,
    };

    #[test]
    fn normalize_base_defaults_the_scheme_and_trims() {
        assert_eq!(normalize_base("localhost:2739/"), "http://localhost:2739");
        assert_eq!(
            normalize_base("http://127.0.0.1:2739"),
            "http://127.0.0.1:2739"
        );
    }

    #[test]
    fn plain_role_strips_the_at_and_whitespace() {
        assert_eq!(plain_role(" @backend "), "backend");
        assert_eq!(plain_role("frontend"), "frontend");
    }

    #[test]
    fn a_role_resolves_to_its_direct_channel_and_a_bad_one_errors() {
        assert_eq!(
            role_channel("backend", "command").unwrap().name().as_str(),
            "@backend"
        );
        assert!(
            role_channel("frontend+backend", "command").is_err(),
            "a pair is not a single role to command",
        );
    }

    #[test]
    fn default_commander_falls_back_to_commander_and_strips_the_at() {
        assert_eq!(default_commander(None), "commander");
        assert_eq!(default_commander(Some("  ")), "commander");
        assert_eq!(default_commander(Some(" @lead ")), "lead");
    }

    #[test]
    fn a_brief_defaults_to_the_commander_and_resolves_channels() {
        // No target: the free-form brief reaches the commander (issue #118).
        assert_eq!(
            brief_target(None, None, None).unwrap().name().as_str(),
            "@commander",
        );
        // A custom commander name is honored for the default brief.
        assert_eq!(
            brief_target(None, None, Some("lead"))
                .unwrap()
                .name()
                .as_str(),
            "@lead",
        );
        // A named role wins over the default.
        assert_eq!(
            brief_target(Some("backend"), None, None)
                .unwrap()
                .name()
                .as_str(),
            "@backend",
        );
        // A channel name broadcasts.
        assert_eq!(
            brief_target(None, Some("all-units"), None)
                .unwrap()
                .name()
                .as_str(),
            "all-units",
        );
        // An unroutable target is an error, not a silent misfire.
        assert!(
            brief_target(None, Some("nonsense"), None).is_err(),
            "an unrecognized channel does not resolve",
        );
    }

    #[test]
    fn the_commander_notice_names_the_role_and_the_order_and_says_it_is_informed() {
        let notice = commander_notice("backend", "switch to the login bug");
        assert!(notice.contains("backend") && notice.contains("switch to the login bug"));
        assert!(
            notice.contains("informed, not bypassed"),
            "the commander is informed, not bypassed: {notice}",
        );
    }

    #[test]
    fn the_reassign_notices_name_the_task_the_roles_and_the_standing() {
        // The old owner is told to hand off, naming the task and the new owner.
        let old = reassign_old_owner_notice("login", "frontend");
        assert!(old.contains("login") && old.contains("frontend"));
        assert!(
            old.contains("Hand it off") && old.contains("no longer own"),
            "the old owner is told to hand off: {old}",
        );

        // The new owner is told it now holds the task, from whom, and where it stands.
        let new = reassign_new_owner_notice("login", "backend", "in_progress");
        assert!(new.contains("login") && new.contains("backend") && new.contains("in_progress"));
        assert!(
            new.contains("Pick it up"),
            "the new owner is told to pick it up: {new}",
        );
        // With no known state, the notice still reads cleanly (no dangling parens).
        let stateless = reassign_new_owner_notice("login", "backend", "");
        assert!(
            !stateless.contains("()") && !stateless.contains("``"),
            "a missing state leaves no empty standing: {stateless}",
        );

        // The commander notice names both roles and says it is informed, not bypassed.
        let commander = reassign_commander_notice("login", "backend", "frontend");
        assert!(commander.contains("backend") && commander.contains("frontend"));
        assert!(
            commander.contains("informed, not bypassed"),
            "the commander is informed, not bypassed: {commander}",
        );
    }
}
