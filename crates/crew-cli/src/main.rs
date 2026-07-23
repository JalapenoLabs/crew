//! The `crew` command-line front-end.
//!
//! The human front-end to the crew substrate: `crew up` brings a crew online,
//! `crew send` posts as the General, `crew watch` tails the conversation, and
//! `crew down` stands the crew down (see `docs/architecture.md`).
//!
//! `main` establishes the application conventions (issue #4): eyre errors, the
//! mimalloc allocator, and the shared structured-logging init, then dispatches the
//! parsed command. `crew up` and `crew down` land here (issue #26); `crew send` and
//! `crew watch` follow.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::Result;
use mimalloc::MiMalloc;

mod down;
mod paths;
mod up;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Command a unit of role-scoped agents as if you were a general directing a team.
#[derive(Debug, Parser)]
#[command(name = "crew", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bring the whole unit online from the crew config, with roles assigned.
    Up {
        /// The crew config to read. Defaults to `./crew.toml`, then the default crew.
        #[arg(short, long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
    /// Stand the running crew down gracefully: stop the agents and deregister them.
    Down,
}

fn main() -> Result<()> {
    crew_telemetry::init();

    match Cli::parse().command {
        Command::Up { config } => up::run(config.as_deref()),
        Command::Down => down::run(),
    }
}
