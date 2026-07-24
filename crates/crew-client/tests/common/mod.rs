//! Shared harness for the crew-client integration tests.
//!
//! Each test starts a real `crewd` in-process on an ephemeral loopback port and
//! drives the synchronous `ureq`-based [`Broker`](crew_client::Broker) client
//! over HTTP, so the test body stays synchronous. This is the one place the
//! spin-up lives, so the harness (graceful shutdown, port retry) evolves once.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    thread,
};

use crew_broker::{AppState, Config};

/// Starts a broker over a fresh in-memory store, returning the address it
/// serves on.
///
/// The socket is bound synchronously so the address is known before the runtime
/// thread starts, then handed to tokio inside the thread. The serve thread is
/// detached: it lives until the test process exits, which is all a
/// request/response test needs (none hold a long-lived stream open).
pub fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let state = AppState::new(Config::default());
            let _ = crew_broker::serve(listener, state, std::future::pending::<()>()).await;
        });
    });
    addr
}
