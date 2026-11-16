//! `TCP_NODELAY` on every connection the server's listeners accept.
//!
//! Neither `axum::serve` over a bare `TcpListener` nor tonic's
//! `TcpIncoming::from` sets it, so a response written in more than one
//! segment holds its last segment until the client acknowledges the first,
//! and a client that delays its ACKs adds that delay to the response.

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

/// A tonic incoming stream over `listener` with `TCP_NODELAY` set on each
/// connection it accepts.
pub(crate) fn grpc_incoming(
    listener: tokio::net::TcpListener,
) -> tonic::transport::server::TcpIncoming {
    tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use axum::serve::Listener;
    use futures::StreamExt;

    use super::*;

    async fn bound() -> (tokio::net::TcpListener, std::net::SocketAddr) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        (listener, addr)
    }

    /// The control for the two tests below: a connection a bare listener
    /// accepts has `TCP_NODELAY` off, so finding it on is the wrapper's doing.
    #[tokio::test]
    async fn a_bare_listener_accepts_without_nodelay() {
        let (listener, addr) = bound().await;
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (stream, _) = listener.accept().await.expect("accept");
        let _client = client.await.expect("join").expect("connect");
        assert!(!stream.nodelay().expect("read TCP_NODELAY"));
    }

    #[tokio::test]
    async fn the_http_listener_sets_nodelay_on_what_it_accepts() {
        let (listener, addr) = bound().await;
        let mut listener = nodelay(listener);
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (stream, _) = listener.accept().await;
        let _client = client.await.expect("join").expect("connect");
        assert!(stream.nodelay().expect("read TCP_NODELAY"));
    }

    #[tokio::test]
    async fn the_grpc_incoming_sets_nodelay_on_what_it_accepts() {
        let (listener, addr) = bound().await;
        let mut incoming = grpc_incoming(listener);
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let stream = incoming
            .next()
            .await
            .expect("one connection")
            .expect("accept");
        let _client = client.await.expect("join").expect("connect");
        assert!(stream.nodelay().expect("read TCP_NODELAY"));
    }
}
