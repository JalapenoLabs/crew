//! `crewd`: the crew message broker daemon.
//!
//! A thin entry point: initialize structured logging, load the configuration from
//! the environment, and run the broker (see [`crew_broker`]) until a shutdown
//! signal.

use eyre::Result;
use mimalloc::MiMalloc;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    crew_telemetry::init();
    let config = crew_broker::Config::from_env()?;
    crew_broker::run(config).await
}
