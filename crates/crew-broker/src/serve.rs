//! Binding the listener, serving the HTTP surface, and graceful shutdown.

use std::future::Future;

use eyre::{eyre, Result, WrapErr};
use tokio::net::TcpListener;
use tracing::{event, Level};

use std::sync::Arc;

use crate::api;
use crate::config::{is_bind_allowed, Config};
use crate::state::AppState;
use crate::store::LogStore;

/// Runs the broker until a shutdown signal, then returns.
///
/// Refuses a non-loopback bind unless [`Config::allow_non_local`] is set, ensures
/// the state directory exists, binds the configured address, serves the HTTP
/// surface, and shuts down gracefully on Ctrl-C or (on Unix) `SIGTERM`.
///
/// # Errors
/// Returns an error if the configured address is non-loopback and not opted in,
/// if the state directory cannot be created, if the address cannot be bound, or
/// if the server errors while running.
pub async fn run(config: Config) -> Result<()> {
    let addr = config.bind_addr();
    if !is_bind_allowed(addr.ip(), config.allow_non_local) {
        return Err(eyre!(
            "refusing to bind non-loopback address {addr}; \
             set CREW_BROKER_ALLOW_NON_LOCAL=1 to allow it"
        ));
    }

    tokio::fs::create_dir_all(&config.state_dir)
        .await
        .wrap_err_with(|| format!("could not create state dir {}", config.state_dir.display()))?;

    // Open the durable log rooted at the state directory, replaying any prior events.
    let storage = Arc::new(LogStore::open(&config.state_dir)?);

    let listener = TcpListener::bind(addr)
        .await
        .wrap_err_with(|| format!("could not bind {addr}"))?;

    let state = AppState::with_storage(config, storage);
    serve(listener, state, shutdown_signal()).await
}

/// Serves the HTTP surface on `listener` until `shutdown` resolves.
async fn serve(
    listener: TcpListener,
    state: AppState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let local = listener
        .local_addr()
        .wrap_err("the listener has no local address")?;
    event!(
        name: "broker.serve.listening",
        Level::INFO,
        server.address = %local,
        crew.storage = state.storage.backend(),
        "crewd listening on {{server.address}}",
    );
    axum::serve(listener, api::build(state))
        .with_graceful_shutdown(shutdown)
        .await
        .wrap_err("the crewd server exited with an error")
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

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::serve;
    use crate::config::Config;
    use crate::state::AppState;

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
