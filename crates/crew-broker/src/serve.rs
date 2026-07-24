//! Binding the listener, serving the HTTP surface, and graceful shutdown.

use std::{future::Future, sync::Arc, time::Duration};

use crew_core::Timestamp;
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

/// How often the broker prunes aged-out events from the log (issue #201).
///
/// Retention is coarse (events age out over hours or days), so an hourly sweep
/// bounds the broker's memory and log promptly while the scan stays cheap: one
/// pass over the in-memory index, and a file rewrite only when something is
/// actually dropped.
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// How long graceful shutdown waits for in-flight connections to finish before
/// forcing a stop (issue #204).
///
/// A request/response connection drains well within this, so it finishes
/// cleanly. An SSE subscriber (`/inbox`, `/stream`) holds its connection open
/// indefinitely and never would, so without a bound crewd would not exit while
/// any `crew watch` or inbox subscriber stays connected. Short enough that
/// `crew down` feels prompt, long enough not to cut off a genuine in-flight
/// response.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

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
/// `shutdown` resolves but bounded by a short grace so an open SSE subscriber
/// cannot stall the exit (issue #204).
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
/// When `shutdown` resolves it drains in-flight connections, but bounded: an
/// SSE subscriber (`/inbox`, `/stream`) holds its connection open indefinitely,
/// so a fully graceful drain would never complete while a watcher is connected.
/// After a short grace period any still-open connection is forced closed, so
/// crewd always exits promptly (issue #204).
///
/// # Errors
/// Returns an error if the listener has no local address or the server exits
/// with an error while running. A forced stop after the shutdown grace is not
/// an error.
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

    // A background sweep prunes aged-out events so the broker's memory and log
    // stay bounded on a long-running unit (issue #201). Tied to the server's
    // lifetime the same way, and a no-op when retention is disabled.
    let pruner = tokio::spawn(retention_sweeper(state.clone()));

    // A durable backend persists in the background (issue #206); keep a handle so
    // its writer can be drained once the server stops, before returning.
    let storage = Arc::clone(&state.storage);

    // Serve on a task so the caller's `shutdown` is awaited alongside it and the
    // graceful drain can then be bounded. The `drain` trigger starts axum's
    // graceful shutdown; `SHUTDOWN_GRACE` bounds it so a long-lived SSE stream
    // cannot stall the exit (issue #204). The task is wrapped so that cancelling
    // `serve` itself stops the server rather than detaching it.
    let (drain, drained) = tokio::sync::oneshot::channel::<()>();
    let mut server = AbortOnDrop(tokio::spawn(async move {
        axum::serve(listener, api::build(state))
            .with_graceful_shutdown(async move {
                let _ = drained.await;
            })
            .await
    }));

    // Serve until the caller signals shutdown, then start the bounded drain.
    shutdown.await;
    let _ = drain.send(());

    let result = match tokio::time::timeout(SHUTDOWN_GRACE, &mut server.0).await {
        // Drained (or errored) within the grace: a real serve error surfaces; a
        // task that was cancelled or panicked at shutdown counts as stopped.
        Ok(joined) => {
            joined.map_or_else(|_join| Ok(()), |served| served.map_err(ServeError::serve))
        }
        // A long-lived stream outlasted the grace: force the stop so crewd exits.
        // `server` dropping aborts the task; log why the drain was cut short.
        Err(_elapsed) => {
            event!(
                name: "broker.serve.forced_shutdown",
                Level::WARN,
                server.address = %local,
                crew.grace_secs = SHUTDOWN_GRACE.as_secs(),
                "shut down after the {{crew.grace_secs}}s grace with connections still open \
                 (an SSE subscriber); forcing the stop",
            );
            Ok(())
        }
    };

    sweeper.abort();
    pruner.abort();
    // Flush events still in the writer's queue so a shutdown mid-burst reaches
    // disk before the process exits. A no-op for a synchronous backend.
    storage.flush();
    result
}

/// A server task that is aborted when dropped, so cancelling [`serve`] stops
/// the server rather than detaching it (a plain
/// [`JoinHandle`](tokio::task::JoinHandle) drop only detaches, leaving the
/// task, and its bound port, running).
struct AbortOnDrop(tokio::task::JoinHandle<std::io::Result<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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

/// Prunes aged-out events on a timer, when retention is configured.
///
/// Returns at once when retention is disabled ([`Config::retention_window`] is
/// `None`), so the task simply ends and its abort handle is a no-op.
async fn retention_sweeper(state: AppState) {
    let Some(window) = state.config.retention_window else {
        return;
    };
    let mut ticker = tokio::time::interval(RETENTION_SWEEP_INTERVAL);
    loop {
        ticker.tick().await;
        sweep_retention(&state, window);
    }
}

/// One retention pass: prunes events older than `window`, returning how many it
/// dropped.
///
/// Split from the timer loop so the behavior is unit-testable without waiting
/// on the ticker. A prune of nothing is silent; a real prune logs the count.
fn sweep_retention(state: &AppState, window: Duration) -> usize {
    let Some(before) = retention_cutoff(window) else {
        return 0;
    };
    let pruned = state.storage.retain(before);
    if pruned > 0 {
        event!(
            name: "broker.retention.pruned",
            Level::INFO,
            crew.events = pruned,
            "pruned {{crew.events}} aged-out events from the log",
        );
    }
    pruned
}

/// The retention cutoff `window` before now: an event older than this is
/// prunable.
///
/// Computed through the Unix-epoch parts so it needs no timestamp arithmetic on
/// the wrapper; `None` only if the subtraction leaves the representable range,
/// in which case the sweep keeps everything rather than pruning on a bad
/// cutoff.
fn retention_cutoff(window: Duration) -> Option<Timestamp> {
    let (secs, nanos) = Timestamp::now().to_unix();
    let window_secs = i64::try_from(window.as_secs()).ok()?;
    Timestamp::from_unix(secs.checked_sub(window_secs)?, nanos)
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
    use std::{net::Ipv4Addr, time::Duration};

    use crew_core::{
        ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
        Timestamp,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::{serve, sweep_retention, sweep_usage};
    use crate::{config::Config, state::AppState};

    /// Parses an RFC 3339 instant into a `Timestamp` for a test fixture.
    fn at(rfc3339: &str) -> crew_core::Timestamp {
        serde_json::from_value(serde_json::Value::String(rfc3339.to_owned())).unwrap()
    }

    /// An event of `kind` from a role on `all-units`, stamped at `ts`.
    fn event(kind: EventKind, ts: Timestamp) -> Event {
        Event {
            ts,
            from: Sender::Role(RoleId::new("backend")),
            channel: ChannelId::new("all-units"),
            task: None,
            kind,
        }
    }

    #[tokio::test]
    async fn a_retention_sweep_prunes_an_aged_ephemeral_event_but_keeps_state() {
        // The sweep threads its cutoff into the store's kind-aware retention
        // (issue #201): an old `message` ages out, while an old `lifecycle` event
        // a projection rebuilds is kept regardless.
        let state = AppState::new(Config::default());
        let old = at("2000-01-01T00:00:00Z");
        state.publish(event(
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: "aged chatter".to_owned(),
            }),
            old,
        ));
        state.publish(event(EventKind::Lifecycle(Lifecycle::Started), old));

        // A one-hour window makes both events "aged", so only the kind decides.
        let pruned = sweep_retention(&state, Duration::from_secs(60 * 60));
        assert_eq!(pruned, 1, "the aged message is pruned");

        let kinds: Vec<_> = state.storage.events().into_iter().map(|e| e.kind).collect();
        assert!(
            matches!(kinds.as_slice(), [EventKind::Lifecycle(_)]),
            "the state-bearing lifecycle event survives; the message does not: {kinds:?}",
        );
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
