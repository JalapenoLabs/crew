//! The `crew` command-line front-end.
//!
//! The human front-end to the crew substrate: `crew up` brings a crew online,
//! `crew send` posts as the General, `crew watch` tails the conversation, and
//! `crew down` stands the crew down (see `docs/architecture.md`).
//!
//! This is the scaffold from issue #1. The command surface lands in later phases;
//! for now `main` only proves the workspace wires together and builds.

use eyre::Result;
use mimalloc::MiMalloc;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[expect(
    clippy::unnecessary_wraps,
    reason = "the eyre Result is the intended app entry signature (M-APP-ERROR); \
              main becomes genuinely fallible once command dispatch lands"
)]
fn main() -> Result<()> {
    println!("crew: scaffold in place; the command surface arrives in later phases.");
    Ok(())
}
