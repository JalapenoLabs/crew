//! The `crew` command-line front-end.
//!
//! The human front-end to the crew substrate (see `docs/architecture.md`). This
//! phase (issue #15) is the dogfood path: `crew send` posts a message to the unit as
//! the General, and `crew watch` tails the conversation live with routing visible.
//! `crew up` / `crew down` (the supervisor front-end) come in a later phase.
//!
//! The broker address comes from `--broker`, else the broker's own environment
//! (`CREW_BROKER_HOST` / `CREW_BROKER_PORT`), else the loopback default.

mod broker;

use clap::{Args, Parser, Subcommand};
use eyre::Result;
use mimalloc::MiMalloc;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// The channel `crew send` posts to by default: the commander, direct.
const DEFAULT_CHANNEL: &str = "@commander";

/// The `crew` command-line front-end.
#[derive(Debug, Parser)]
#[command(name = "crew", version, about = "The crew command-line front-end.")]
struct Cli {
    /// The broker base URL (default: `CREW_BROKER_HOST`/`PORT`, else the loopback bind).
    #[arg(long, global = true, value_name = "URL")]
    broker: Option<String>,
    #[command(subcommand)]
    command: Command,
}

/// The `crew` subcommands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Post a message to the unit as the General.
    Send(SendArgs),
    /// Tail the conversation live, with routing visible.
    Watch(WatchArgs),
}

/// Arguments for `crew send`.
#[derive(Debug, Args)]
struct SendArgs {
    /// Send directly to a role (its `@role` channel).
    #[arg(long, conflicts_with = "channel", value_name = "ROLE")]
    to: Option<String>,
    /// Send to a named channel (for example `all-units` or `frontend+backend`).
    #[arg(long, value_name = "NAME")]
    channel: Option<String>,
    /// The message body (the positional words, joined with spaces).
    ///
    /// `--to` / `--channel` may appear before, after, or among the words, so
    /// `crew send ship it --to backend` works. A body that starts with `-` must
    /// follow a `--` separator.
    #[arg(required = true, value_name = "MESSAGE")]
    message: Vec<String>,
}

/// Arguments for `crew watch`.
#[derive(Debug, Args)]
struct WatchArgs {
    /// Watch one role's self-filtered inbox instead of the whole firehose.
    #[arg(long, value_name = "ROLE")]
    role: Option<String>,
}

fn main() -> Result<()> {
    crew_telemetry::init();
    let cli = Cli::parse();
    let base = broker_url(cli.broker.as_deref())?;

    match cli.command {
        Command::Send(args) => {
            let channel = resolve_channel(args.to.as_deref(), args.channel.as_deref());
            broker::send(&base, &channel, &args.message.join(" "))
        }
        Command::Watch(args) => broker::watch(&base, &watch_path(args.role.as_deref())),
    }
}

/// The broker base URL, from the flag or the broker's environment configuration.
///
/// # Errors
/// Returns an error if `CREW_BROKER_HOST` or `CREW_BROKER_PORT` is set but invalid.
fn broker_url(flag: Option<&str>) -> Result<String> {
    if let Some(url) = flag {
        return Ok(normalize_base(url));
    }
    let config = crew_broker::Config::from_env()?;
    Ok(format!("http://{}", config.bind_addr()))
}

/// Normalizes a `--broker` value: default the scheme to `http`, drop a trailing slash.
fn normalize_base(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_owned()
    } else {
        format!("http://{url}")
    }
}

/// The channel a `send` targets: `--to <role>` becomes `@role`, `--channel <name>`
/// is used as is, and neither defaults to the commander.
fn resolve_channel(to: Option<&str>, channel: Option<&str>) -> String {
    match (to, channel) {
        (Some(role), _) => format!("@{}", role.trim()),
        (None, Some(name)) => name.trim().to_owned(),
        (None, None) => DEFAULT_CHANNEL.to_owned(),
    }
}

/// The SSE path a `watch` reads: one role's inbox, or the whole firehose.
fn watch_path(role: Option<&str>) -> String {
    match role {
        Some(role) => format!("/inbox?role={}", role.trim()),
        None => "/stream".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::{normalize_base, resolve_channel, watch_path, Cli, DEFAULT_CHANNEL};

    #[test]
    fn the_command_tree_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn resolve_channel_defaults_to_the_commander() {
        assert_eq!(resolve_channel(None, None), DEFAULT_CHANNEL);
    }

    #[test]
    fn resolve_channel_maps_a_role_to_its_direct_channel() {
        assert_eq!(resolve_channel(Some("backend"), None), "@backend");
    }

    #[test]
    fn resolve_channel_uses_a_named_channel_verbatim() {
        assert_eq!(resolve_channel(None, Some("all-units")), "all-units");
    }

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

    #[test]
    fn watch_path_is_the_firehose_or_a_role_inbox() {
        assert_eq!(watch_path(None), "/stream");
        assert_eq!(watch_path(Some("backend")), "/inbox?role=backend");
    }
}
