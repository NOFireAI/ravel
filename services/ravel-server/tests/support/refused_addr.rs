//! A loopback address whose connections are refused for the life of the test
//! process. Included with `#[path]` by each test that needs a dead endpoint.
//!
//! Binding `127.0.0.1:0`, reading the port and dropping the listener frees the
//! port for any parallel test to bind before the dial, which then gets a
//! response instead of a refusal. Here the port stays bound by a socket that
//! never listens: a connect to it is refused at once, and with neither
//! `SO_REUSEADDR` nor `SO_REUSEPORT` set no other socket can bind it while the
//! process lives.

#![allow(dead_code, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::OnceLock;

use tokio::net::TcpSocket;

static REFUSED: OnceLock<(TcpSocket, SocketAddr)> = OnceLock::new();

/// The process-wide refused address.
pub fn refused_addr() -> SocketAddr {
    REFUSED
        .get_or_init(|| {
            let socket = TcpSocket::new_v4().expect("refused socket");
            socket.set_reuseaddr(false).expect("clear SO_REUSEADDR");
            socket.set_reuseport(false).expect("clear SO_REUSEPORT");
            socket
                .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .expect("bind refused socket");
            let addr = socket.local_addr().expect("refused local addr");
            (socket, addr)
        })
        .1
}
