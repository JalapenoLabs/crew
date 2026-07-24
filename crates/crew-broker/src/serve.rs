//! Binding the listener, serving the HTTP surface, and graceful shutdown.

use std::{future::Future, sync::Arc, time::Duration};

use tokio::net::TcpListener;
use tracing::{event, Level};

use crate::{
    api,
    config::{is_bind_allowed, Config},
    serve_error::ServeError,
    state::AppState,
    store::LogStore,
    usage::usage_event,
};

/// How often the broker sweeps for a usage auto-pause whose window has reset
/// (issue #112).
///
/// The auto-resume already takes effect lazily the instant the window resets;
/// this only bounds how soon the lift is announced on the stream. Thirty
/// seconds keeps the sweep negligibly cheap (one mutex read per tick) while
/// surfacing the resume promptly.
const USAGE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Runs the broker until a shutdown signal (Ctrl-C or, on Unix, `SIGTERM`).
///
/// This is [`run_until`] wired to the process signal handler; the `crewd`
/// binary uses it. An embedder that owns its own shutdown (for example `crew
/// up`, which stands the broker down alongside the crew) uses [`run_until`]
/// directly.
///
/// # Errors
/// See [`run_until`].
pub async fn run(config: Config) -> Result<(), ServeError> {
    run_until(config, shutdown_signal()).await
}

/// Runs the broker until `shutdown` resolves, then returns.
///
/// Refuses a non-loopback bind unless [`Config::allow_non_local`] is set,
/// ensures the state directory exists, opens the durable log, binds the
/// configured address, and serves the HTTP surface, draining gracefully when
/// `shutdown` resolves.
///
/// # Errors
/// Returns an error if the configured address is non-loopback and not opted in,
/// if the state directory cannot be created, if the durable log cannot be
/// opened, if the address cannot be bound, or if the server errors while
/// running.
pub async fn run_until(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let addr = config.bind_addr();
    if !is_bind_allowed(addr.ip(), config.allow_non_local) {
        return Err(ServeError::non_local_bind(addr));
    }

    tokio::fs::create_dir_all(&config.state_dir)
        .await
        .map_err(|source| ServeError::state_dir(config.state_dir.clone(), source))?;

    // Open the durable log rooted at the state directory, replaying any prior
    // events.
    let storage = Arc::new(LogStore::open(&config.state_dir).map_err(ServeError::log)?);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| ServeError::bind(addr, source))?;

    let state = AppState::with_storage(config, storage);
    serve(listener, state, shutdown).await
}

/// Serves the broker's HTTP surface on `listener` until `shutdown` resolves.
///
/// The building block behind [`run`]: an embedder (or an integration test)
/// binds its own listener and supplies a custom [`AppState`], for example to
/// serve on an ephemeral port or over a specific [`Storage`](crate::Storage)
/// backend, and drives shutdown with its own future.
///
/// # Errors
/// Returns an error if the listener has no local address or the server exits
/// with an error.
pub async fn serve(
    listener: TcpListener,
    state: AppState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let local = listener.local_addr().map_err(ServeError::local_addr)?;
    event!(
        name: "broker.serve.listening",
        Level::INFO,
        server.address = %local,
        crew.storage = state.storage.backend(),
        "crewd listening on {{server.address}}",
    );

    // A background sweep announces a usage auto-pause lifting when its window
    // resets (issue #112). Tied to the server's lifetime: it is aborted when
    // serving ends, so it never outlives the broker it reports for.
    let sweeper = tokio::spawn(usage_sweeper(state.clone()));

    // A durable backend persists in the background (issue #206); keep a handle so
    // its writer can be drained once the server stops, before returning.
    let storage = Arc::clone(&state.storage);
    let result = axum::serve(listener, api::build(state))
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::serve);

    sweeper.abort();
    // Flush events still in the writer's queue so a shutdown mid-burst reaches
    // disk before the process exits. A no-op for a synchronous backend.
    storage.flush();
    result
}

/// Sweeps for an expired usage auto-pause on a timer, announcing each lift.
async fn usage_sweeper(state: AppState) {
    let mut ticker = tokio::time::interval(USAGE_SWEEP_INTERVAL);
    loop {
        ticker.tick().await;
        sweep_usage(&state);
    }
}

/// One sweep: if a usage auto-pause has expired, clear it and publish the lift.
///
/// Returns whether it announced a lift, so the behavior is unit-testable
/// without waiting on the ticker.
fn sweep_usage(state: &AppState) -> bool {
    let Some(view) = state.expire_usage_pause() else {
        return false;
    };
    state.publish(usage_event(&view));
    event!(
        name: "broker.usage.autoresumed",
        Level::INFO,
        usage.percent = view.percent,
        "usage window reset; new work auto-resumed",
    );
    true
}

/// Resolves when the process receives Ctrl-C or (on Unix) `SIGTERM`.
async fn shutdown_signal() {
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
    event!(
        name: "broker.shutdown.signal",
        Level::INFO,
        "shutdown signal received; draining connections",
    );
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::{serve, sweep_usage};
    use crate::{config::Config, state::AppState};

    /// Parses an RFC 3339 instant into a `Timestamp` for a test fixture.
    fn at(rfc3339: &str) -> crew_core::Timestamp {
        serde_json::from_value(serde_json::Value::String(rfc3339.to_owned())).unwrap()
    }

    #[tokio::test]
    async fn a_sweep_announces_an_expired_usage_pause_on_the_stream_once() {
        use crew_core::EventKind;

        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        // Arm a pause whose window has already reset: armed, but lazily lifted, so no
        // lift has reached the stream yet.
        let _ = state.report_usage(99, at("2000-01-01T00:00:00Z"));
        assert!(!state.is_usage_paused());

        // The sweep clears the expired pause and announces the lift, un-paused.
        assert!(sweep_usage(&state), "the expired pause is swept");
        let streamed = stream.try_recv().unwrap().event;
        let EventKind::Usage(usage) = streamed.kind else {
            panic!("expected a usage lift event");
        };
        assert!(!usage.paused, "the auto-resume is announced as un-paused");
        assert_eq!(usage.percent, 99);

        // A second sweep finds nothing: no duplicate lift event.
        assert!(!sweep_usage(&state));
        assert!(
            stream.try_recv().is_err(),
            "the lift is announced exactly once",
        );
    }

    #[tokio::test]
    async fn serves_health_then_shuts_down_cleanly() {
        // Bind an ephemeral loopback port and serve on it.
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve(listener, AppState::new(Config::default()), async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        // Probe the health endpoint with a bare HTTP/1.1 request.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(
            response.contains("200 OK"),
            "health should be 200: {response}"
        );
        assert!(
            response.contains("\"status\":\"ok\""),
            "health body: {response}"
        );
        assert!(
            response.contains("\"service\":\"crewd\""),
            "health body: {response}"
        );

        // Signal graceful shutdown and confirm the server returns without error.
        shutdown_tx.send(()).unwrap();
        server
            .await
            .unwrap()
            .expect("the server should shut down cleanly");
    }
}
