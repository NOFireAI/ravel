//! `TCP_NODELAY` on every connection the router's listeners accept.
//!
//! `axum::serve` over a bare `TcpListener` does not set it, so a response
//! written in more than one segment holds its last segment until the client
//! acknowledges the first, and a client that delays its ACKs adds that delay
//! to the response.

use axum::serve::ListenerExt;

/// `listener` with `TCP_NODELAY` set on each connection it accepts. A
/// connection that refuses the option is served without it.
pub(crate) fn nodelay(
    listener: tokio::net::TcpListener,
) -> impl axum::serve::Listener<Io = tokio::net::TcpStream, Addr = std::net::SocketAddr> {
    listener.tap_io(|stream| {
        if let Err(err) = stream.set_nodelay(true) {
            tracing::debug!(error = %err, "could not set TCP_NODELAY on an accepted connection");
        }
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use axum::serve::Listener;

    use super::*;

    /// A bare listener's accepted connection has the option off, so finding
    /// it on through [`nodelay`] is the wrapper's doing.
    #[tokio::test]
    async fn the_listener_sets_nodelay_on_what_it_accepts() {
        let bare = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = bare.local_addr().expect("local addr");
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (plain, _) = bare.accept().await.expect("accept");
        let _client = client.await.expect("join").expect("connect");
        assert!(!plain.nodelay().expect("read TCP_NODELAY"));

        let mut wrapped = nodelay(bare);
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (stream, _) = wrapped.accept().await;
        let _client = client.await.expect("join").expect("connect");
        assert!(stream.nodelay().expect("read TCP_NODELAY"));
    }
}
