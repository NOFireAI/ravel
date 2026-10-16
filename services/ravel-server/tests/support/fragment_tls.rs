//! Dedicated fragment listener test material shared by the integration tests
//! that run `--distributed-query`, which requires `--fragment-listener`
//! (ADR-1689 decision 4). Included with `#[path]` by each test that needs it.

#![allow(dead_code, clippy::expect_used)]

use ravel_server::config::FragmentListenerSettings;

// Operator-provisioned test PEM material, generated offline (EC P-256, valid to
// 2126), the same material the dedicated-listener unit tests in `distrib.rs`
// use: a CA, and a `ravel-fragment` leaf it signed carrying both serverAuth and
// clientAuth, since one process is both the worker that serves the listener
// and the coordinator that dials it.
pub const TEST_FRAGMENT_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBiDCCAS6gAwIBAgIURqm7z2RSSm9YMcJrjixkBTS6iSAwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MTkyMTIx
MzVaGA8yMTI2MDgyNjIxMjEzNVowITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQt
dGVzdC1jYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABOynQpfkkGc1dJv+181e
8I9uvBML0AvXJo95Z4dxje72IOA/Hhh4cpQ0EQfogGW4LtnbWS7NgilX1+RpC6gG
CVajQjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQW
BBT7C6f70yaChMoXGTIBoZSw+8p3TTAKBggqhkjOPQQDAgNIADBFAiBXCp1E6E6i
I3VH8wGfxQxywXkuQ86dVH5Z7FpTA9udVAIhAJT+wKFJo9hWpeKKbEmbtuuwfaok
5axPjJ9kiO1C6ZIu
-----END CERTIFICATE-----
";
pub const TEST_FRAGMENT_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIB2jCCAYCgAwIBAgIUX3yTIiYvkWMVICeYkAoWQ/cpCEYwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MTkyMTIx
MzVaGA8yMTI2MDgyNjIxMjEzNVowGTEXMBUGA1UEAwwOcmF2ZWwtZnJhZ21lbnQw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQHF0p+BdFVa6wOH4/e9vBYV2a3We8/
+XoQmKdUGN8vOrlnREuOj4pqI54CjnYZ1OLRQF3JRynJ5y/yWL+i3rFEo4GbMIGY
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUF
BwMBBggrBgEFBQcDAjAZBgNVHREEEjAQgg5yYXZlbC1mcmFnbWVudDAdBgNVHQ4E
FgQUfJC6GQoihnxgaXOnWiJBAfwInPwwHwYDVR0jBBgwFoAU+wun+9MmgoTKFxky
AaGUsPvKd00wCgYIKoZIzj0EAwIDSAAwRQIgbEMg/jES94eo3dxOwEiM1FiHhY1v
hzdk6C9qmCCckI4CIQC/2tvVzC1VvE9eO0Y9eN2GDp63hSc+5YvKnvFm8P6I6Q==
-----END CERTIFICATE-----
";
pub const TEST_FRAGMENT_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgonxTB6rEt10ZCoQ+
L1ACQVzux8AvoQAB2A9c890hWPmhRANCAAQHF0p+BdFVa6wOH4/e9vBYV2a3We8/
+XoQmKdUGN8vOrlnREuOj4pqI54CjnYZ1OLRQF3JRynJ5y/yWL+i3rFE
-----END PRIVATE KEY-----
";

/// The resolved `--fragment-listener` settings over the test material, bound
/// to an ephemeral loopback port.
pub fn listener_settings() -> FragmentListenerSettings {
    FragmentListenerSettings {
        addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
        tls_cert_pem: TEST_FRAGMENT_CERT_PEM.as_bytes().to_vec(),
        tls_key_pem: TEST_FRAGMENT_KEY_PEM.as_bytes().to_vec(),
        tls_ca_pem: TEST_FRAGMENT_CA_PEM.as_bytes().to_vec(),
    }
}

/// The coordinator-side dial configuration a process with
/// [`listener_settings`] builds.
pub fn client_tls() -> tonic::transport::ClientTlsConfig {
    let _ = rustls::crypto::ring::default_provider().install_default();
    ravel_server::distrib::fragment_client_tls(&listener_settings())
}

/// A server builder terminating mutual TLS with the test material, as the
/// dedicated fragment listener does.
pub fn tls_server() -> tonic::transport::Server {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tonic::transport::Server::builder()
        .tls_config(
            tonic::transport::ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(
                    TEST_FRAGMENT_CERT_PEM,
                    TEST_FRAGMENT_KEY_PEM,
                ))
                .client_ca_root(tonic::transport::Certificate::from_pem(
                    TEST_FRAGMENT_CA_PEM,
                )),
        )
        .expect("server TLS config")
}

/// A `SeriesFetch` client over mutual TLS to a dedicated listener at `addr`.
pub async fn dial(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("https://{addr}"))
        .expect("valid endpoint uri")
        .tls_config(client_tls())
        .expect("client TLS config")
        .connect()
        .await
        .expect("TLS dial to the dedicated fragment listener")
}
