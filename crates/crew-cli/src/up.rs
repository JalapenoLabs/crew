//! `crew up`: bring the whole unit online from the crew config (issue #26).
//!
//! The headline experience: one command reads the config, starts the broker if
//! one is not already running, launches a lifecycle-managed fleet with every
//! role assigned, surfaces the live roster and the commander entry point, and
//! holds the unit online until interrupted. On Ctrl-C, `SIGTERM`, or `crew
//! down`, it stands the crew down gracefully, so no agent process is left
//! orphaned.
//!
//! `crew up` runs in the foreground and owns the crew: it holds the fleet (its
//! driver threads) and, when it started one, the in-process broker. Standing
//! down tears both down together, so what `crew up` brought online, standing
//! down removes.

use std::{
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crew_substrate::{
    broker::Config as BrokerConfig,
    core::{BrokerEndpoint, CrewConfig},
    supervisor::{AgentState, Fleet, RosterClient, Supervisor},
};
use eyre::{eyre, Result, WrapErr};
use tokio::sync::oneshot;
use tracing::{event, Level};

use crate::paths::pidfile;

/// How long to wait for the broker to accept connections and for the roles to
/// register.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Brings the crew online and holds it there until a shutdown signal.
///
/// # Errors
/// Returns an error if the config cannot be read or is invalid, the broker
/// cannot be started, or the fleet cannot be launched (a missing MCP server or
/// an unprovisionable role).
pub fn run(config_path: Option<&Path>) -> Result<()> {
    let (crew_config, config_dir) = load_config(config_path)?;

    // The broker: its runtime config (env-overridable) and the endpoint agents
    // reach.
    let broker_config = BrokerConfig::from_env()?;
    let endpoint = BrokerEndpoint::new(broker_config.host.to_string(), broker_config.port);
    let base_url = endpoint.base_url();

    // Start the broker only if one is not already listening, so an operator can run
    // a long-lived `crewd` and bring crews up against it.
    let broker = if broker_healthy(&base_url) {
        event!(
            name: "cli.up.broker_present",
            Level::INFO,
            crew.broker = %base_url,
            "using the broker already listening at {{crew.broker}}",
        );
        None
    } else {
        Some(start_broker(broker_config.clone(), &base_url)?)
    };

    // Bring the unit online: register the MCP server, launch the lifecycle-managed
    // fleet from the config, and start every role so the unit is live and
    // connected.
    let root = broker_config.state_dir.join("agents");
    let supervisor = Supervisor::new(endpoint, root);
    let fleet = supervisor.launch(&crew_config, &config_dir)?;
    fleet.start_all()?;

    // Surface the live roster and the commander entry point once the unit connects.
    let roster = RosterClient::new(base_url);
    await_roster(&roster, &crew_config);
    print_roster(&crew_config, &fleet);
    print_commander_entry(&crew_config);

    // Record the PID so `crew down` (or a signal) can stand this process down.
    let pidfile = pidfile(&broker_config);
    write_pidfile(&pidfile)?;

    // Hold the unit online until interrupted, then stand it down gracefully.
    wait_for_signal();
    event!(name: "cli.up.standing_down", Level::INFO, "standing the crew down");
    fleet.shutdown();
    if let Some(broker) = broker {
        broker.shutdown();
    }
    let _ = std::fs::remove_file(&pidfile);
    println!("Crew is down.");
    Ok(())
}

/// Loads the crew config: the given path, else `./crew.toml`, else the default
/// crew.
fn load_config(path: Option<&Path>) -> Result<(CrewConfig, PathBuf)> {
    let default = Path::new("crew.toml");
    let chosen = match path {
        Some(path) => Some(path),
        None if default.exists() => Some(default),
        None => None,
    };

    let Some(path) = chosen else {
        event!(
            name: "cli.up.config_default",
            Level::INFO,
            "no crew config found; bringing up the default crew",
        );
        // The default crew has no repos, so the anchor is unused; the current
        // directory stands in.
        return Ok((CrewConfig::default(), PathBuf::from(".")));
    };

    let toml = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("could not read crew config {}", path.display()))?;
    let config = CrewConfig::from_toml(&toml)
        .wrap_err_with(|| format!("invalid crew config {}", path.display()))?;
    event!(
        name: "cli.up.config_loaded",
        Level::INFO,
        crew.config = %path.display(),
        crew.roles = config.roles.len(),
        "loaded a {{crew.roles}}-role crew from {{crew.config}}",
    );
    Ok((config, config_dir_of(path)))
}

/// The directory that holds the config file: the anchor a bare `repos` name
/// resolves against (issue #126).
///
/// A bare `crew.toml` with no parent component anchors to the current
/// directory.
fn config_dir_of(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// A broker running in-process on its own thread, with a handle to stand it
/// down.
struct BrokerHandle {
    shutdown: oneshot::Sender<()>,
    thread: JoinHandle<()>,
}

impl BrokerHandle {
    /// Signals the broker to drain, then joins its thread.
    fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.thread.join();
    }
}

/// Starts the broker in-process and waits for it to accept connections.
fn start_broker(config: BrokerConfig, base_url: &str) -> Result<BrokerHandle> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let thread = thread::Builder::new()
        .name("crewd".to_owned())
        .spawn(move || run_broker(config, shutdown_rx))
        .wrap_err("could not spawn the broker thread")?;

    wait_for_health(base_url)?;
    event!(
        name: "cli.up.broker_started",
        Level::INFO,
        crew.broker = %base_url,
        "started the broker at {{crew.broker}}",
    );
    Ok(BrokerHandle {
        shutdown: shutdown_tx,
        thread,
    })
}

/// The broker thread body: build a runtime and serve until the shutdown signal
/// fires.
fn run_broker(config: BrokerConfig, shutdown_rx: oneshot::Receiver<()>) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            event!(
                name: "cli.up.broker_runtime_failed",
                Level::ERROR,
                error = %err,
                "the broker runtime failed to start",
            );
            return;
        }
    };

    let outcome = runtime.block_on(crew_substrate::broker::run_until(config, async move {
        let _ = shutdown_rx.await;
    }));
    if let Err(err) = outcome {
        event!(
            name: "cli.up.broker_exited",
            Level::ERROR,
            error = ?err,
            "the broker exited with an error",
        );
    }
}

/// Whether a broker answers `GET /health` at `base_url`.
fn broker_healthy(base_url: &str) -> bool {
    ureq::get(&format!("{base_url}/health"))
        .timeout(Duration::from_millis(500))
        .call()
        .is_ok()
}

/// Polls `GET /health` until the broker is ready or the ready timeout passes.
fn wait_for_health(base_url: &str) -> Result<()> {
    if wait_until(READY_TIMEOUT, || broker_healthy(base_url)) {
        return Ok(());
    }
    Err(eyre!(
        "the broker did not become ready at {base_url} within {READY_TIMEOUT:?}"
    ))
}

/// Waits for the started roles to register, so the roster printed next is the
/// live unit.
fn await_roster(roster: &RosterClient, config: &CrewConfig) {
    let all_registered = || {
        roster.roles().is_ok_and(|registered| {
            config
                .roles
                .iter()
                .all(|spec| registered.contains(&spec.role))
        })
    };
    // Best-effort: on timeout, fall through and print what came up. The per-role
    // state still reports the truth, so a slow or failed role is visible rather
    // than hidden.
    let _ = wait_until(READY_TIMEOUT, all_registered);
}

/// Polls `condition` until it holds or `timeout` passes, returning whether it
/// held.
fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    condition()
}

/// Prints the live roster: each role, its state, and the lane it owns.
fn print_roster(config: &CrewConfig, fleet: &Fleet) {
    println!("\nCrew is up. {} roles online:", config.roles.len());
    for spec in &config.roles {
        let state = fleet.state(&spec.role).map_or("pending", state_label);
        let lane = if spec.owned_paths.is_empty() {
            "routes, owns no lane".to_owned()
        } else {
            spec.owned_paths.join(", ")
        };
        println!("  {:<12} {state:<8} {lane}", spec.role.as_str());
    }
}

/// A human label for an agent's lifecycle state.
fn state_label(state: AgentState) -> &'static str {
    match state {
        AgentState::Stopped => "stopped",
        AgentState::Working => "working",
        AgentState::Idle => "idle",
        AgentState::Dead => "dead",
    }
}

/// Points the operator at the commander, the entry point for briefing the unit.
fn print_commander_entry(config: &CrewConfig) {
    let commander = config.commander.as_str();
    println!("\nBrief the commander (`{commander}`) to set the unit to work:");
    println!("  crew send \"<your intent>\"");
    println!("\nPress Ctrl-C or run `crew down` to stand the crew down.");
}

/// Writes this process's PID to `path`, creating the state directory if needed.
fn write_pidfile(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .wrap_err_with(|| format!("could not create state dir {}", parent.display()))?;
    }
    std::fs::write(path, std::process::id().to_string())
        .wrap_err_with(|| format!("could not write pidfile {}", path.display()))
}

/// Blocks until the process receives Ctrl-C or (on Unix) `SIGTERM`.
///
/// A small runtime just for the signal; the broker has its own on another
/// thread, so a signal here does not race the broker's own graceful drain.
fn wait_for_signal() {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            event!(
                name: "cli.up.signal_runtime_failed",
                Level::ERROR,
                error = %err,
                "could not build the signal runtime; standing down now",
            );
            return;
        }
    };

    runtime.block_on(async {
        let ctrl_c = async {
            let _ = tokio::signal::ctrl_c().await;
        };

        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut term) = signal(SignalKind::terminate()) {
                term.recv().await;
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {}
            () = terminate => {}
        }
    });
}
