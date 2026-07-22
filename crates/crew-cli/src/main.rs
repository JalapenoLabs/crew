//! The `crew` command-line front-end.
//!
//! The human front-end to the crew substrate: `crew up` brings a crew online,
//! `crew send` posts as the General, `crew watch` tails the conversation, and
//! `crew down` stands the crew down (see `docs/architecture.md`).
//!
//! The command surface lands in later phases; for now `main` establishes the
//! application conventions (issue #4): eyre errors, the mimalloc allocator, and
//! the shared structured-logging init, then emits a boot event.

use eyre::Result;
use mimalloc::MiMalloc;
use tracing::{event, Level};

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[expect(
    clippy::unnecessary_wraps,
    reason = "the eyre Result is the intended app entry signature (M-APP-ERROR); \
              main becomes genuinely fallible once command dispatch lands"
)]
fn main() -> Result<()> {
    crew_telemetry::init();

    // A structured, named boot event (M-LOG-STRUCTURED): the name is
    // `<component>.<operation>.<state>`, the fields carry the data, and the
    // message template references them with `{{...}}` so formatting defers to
    // viewing time.
    event!(
        name: "cli.boot.ready",
        Level::INFO,
        crew.component = "cli",
        crew.version = env!("CARGO_PKG_VERSION"),
        "crew {{crew.component}} ready (version {{crew.version}})",
    );

    Ok(())
}
