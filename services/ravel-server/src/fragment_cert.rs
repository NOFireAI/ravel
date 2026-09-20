//! Startup validation of the fragment listener's TLS identity (ADR-0071
//! amendment decision 1, issue #1690).
//!
//! The dedicated fragment listener is mutual TLS, and one key pair serves both
//! directions: the same `--fragment-tls-cert` this process presents as a server
//! is the client identity it presents when it dials a peer. A peer's rustls
//! client verifier requires the `clientAuth` extended key usage, so a
//! certificate provisioned against the pre-#1690 documentation (`serverAuth`
//! only) still serves inbound fetches while failing every outbound dial at the
//! handshake, and each such failure falls back to coordinator-local execution.
//! Distribution stops with nothing on this process reporting it.
//!
//! [`ensure_client_auth_eku`] turns that into a startup refusal.

use std::path::Path;

use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject as _;
use simple_asn1::{ASN1Block, ASN1Class, OID, oid};

/// `id-ce-extKeyUsage`, the certificate extension listing the purposes a
/// certificate may be used for (RFC 5280 section 4.2.1.12).
fn ext_key_usage_oid() -> OID {
    oid!(2, 5, 29, 37)
}

/// `id-kp-clientAuth`, the usage a TLS client certificate needs.
fn client_auth_oid() -> OID {
    oid!(1, 3, 6, 1, 5, 5, 7, 3, 2)
}

/// `anyExtendedKeyUsage`: a certificate listing it is usable for every purpose,
/// so it satisfies the `clientAuth` requirement.
fn any_extended_key_usage_oid() -> OID {
    oid!(2, 5, 29, 37, 0)
}

/// Refuse startup when the fragment TLS identity at `path` cannot be presented
/// as a client certificate.
///
/// Accepts a certificate whose `extendedKeyUsage` lists `clientAuth` or
/// `anyExtendedKeyUsage`, and one carrying no `extendedKeyUsage` extension at
/// all (RFC 5280: an absent extension constrains nothing, and webpki accepts
/// it for either role). Everything else is the upgrade hazard above.
pub fn ensure_client_auth_eku(path: &Path, cert_pem: &[u8]) -> anyhow::Result<()> {
    let der = CertificateDer::from_pem_slice(cert_pem).map_err(|e| {
        anyhow::anyhow!(
            "failed to read a PEM CERTIFICATE block from --fragment-tls-cert {}: {e}",
            path.display()
        )
    })?;
    let usages = extended_key_usages(der.as_ref()).map_err(|e| {
        anyhow::anyhow!(
            "failed to parse --fragment-tls-cert {} as an X.509 certificate: {e}. Its \
             extendedKeyUsage is read at startup so a certificate that cannot dial a peer is \
             refused here rather than failing every outbound fragment handshake.",
            path.display()
        )
    })?;
    let Some(usages) = usages else {
        // No extension: unconstrained, so it can be presented as either role.
        return Ok(());
    };
    if usages.contains(&client_auth_oid()) || usages.contains(&any_extended_key_usage_oid()) {
        return Ok(());
    }
    anyhow::bail!(
        "--fragment-tls-cert {} is missing the clientAuth extended key usage (it carries: {}). \
         The dedicated fragment listener is mutual TLS (ADR-0071 amendment decision 1): this \
         process presents that same certificate as its client identity on every outbound \
         fragment dial, and a peer's client verifier rejects a certificate without clientAuth, \
         so every dial would fail at the handshake and fall back to coordinator-local execution \
         with nothing failing here. Reissue the certificate with \
         extendedKeyUsage = serverAuth, clientAuth (cert-manager: usages: server auth, client \
         auth) and restart; see docs/guides/operations/deployment.md.",
        path.display(),
        render_usages(&usages)
    );
}

/// The `extendedKeyUsage` OIDs of a DER-encoded X.509 certificate, or `None`
/// when it carries no such extension.
fn extended_key_usages(der: &[u8]) -> anyhow::Result<Option<Vec<OID>>> {
    let blocks = simple_asn1::from_der(der)?;
    let [ASN1Block::Sequence(_, certificate)] = blocks.as_slice() else {
        anyhow::bail!("expected one top-level SEQUENCE (Certificate)");
    };
    let Some(ASN1Block::Sequence(_, tbs)) = certificate.first() else {
        anyhow::bail!("expected a SEQUENCE (TBSCertificate) as the first Certificate field");
    };
    // TBSCertificate's extensions are `[3] EXPLICIT Extensions OPTIONAL`; the
    // version is `[0]`, so the tag is what selects the right field.
    let extensions = tbs.iter().find_map(|field| match field {
        ASN1Block::Explicit(ASN1Class::ContextSpecific, _, tag, inner)
            if *tag == simple_asn1::BigUint::from(3u8) =>
        {
            Some(inner.as_ref())
        }
        _ => None,
    });
    let Some(extensions) = extensions else {
        return Ok(None);
    };
    let ASN1Block::Sequence(_, extensions) = extensions else {
        anyhow::bail!("expected a SEQUENCE inside the [3] extensions field");
    };
    for extension in extensions {
        let ASN1Block::Sequence(_, fields) = extension else {
            continue;
        };
        let Some(ASN1Block::ObjectIdentifier(_, id)) = fields.first() else {
            continue;
        };
        if *id != ext_key_usage_oid() {
            continue;
        }
        // Extension ::= SEQUENCE { extnID, critical BOOLEAN DEFAULT FALSE,
        // extnValue OCTET STRING }; the value is always last.
        let Some(ASN1Block::OctetString(_, value)) = fields.last() else {
            anyhow::bail!("the extendedKeyUsage extension carries no OCTET STRING value");
        };
        let value = simple_asn1::from_der(value)?;
        let [ASN1Block::Sequence(_, purposes)] = value.as_slice() else {
            anyhow::bail!("expected a SEQUENCE of key purpose OIDs in extendedKeyUsage");
        };
        return Ok(Some(
            purposes
                .iter()
                .filter_map(|purpose| match purpose {
                    ASN1Block::ObjectIdentifier(_, id) => Some(id.clone()),
                    _ => None,
                })
                .collect(),
        ));
    }
    Ok(None)
}

/// The usages an operator would recognise in an error, named where RFC 5280
/// names them and dotted otherwise.
fn render_usages(usages: &[OID]) -> String {
    if usages.is_empty() {
        return "no key purposes".to_string();
    }
    usages
        .iter()
        .map(|usage| {
            let dotted = dotted(usage);
            match dotted.as_str() {
                "1.3.6.1.5.5.7.3.1" => "serverAuth".to_string(),
                "1.3.6.1.5.5.7.3.2" => "clientAuth".to_string(),
                "1.3.6.1.5.5.7.3.3" => "codeSigning".to_string(),
                "1.3.6.1.5.5.7.3.4" => "emailProtection".to_string(),
                "1.3.6.1.5.5.7.3.8" => "timeStamping".to_string(),
                "1.3.6.1.5.5.7.3.9" => "OCSPSigning".to_string(),
                _ => dotted,
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// An OID in the dotted-decimal spelling an operator reads in `openssl x509`
/// output.
fn dotted(oid: &OID) -> String {
    match oid.as_vec::<u64>() {
        Ok(arcs) => arcs
            .iter()
            .map(|arc| arc.to_string())
            .collect::<Vec<_>>()
            .join("."),
        Err(_) => "<unreadable OID>".to_string(),
    }
}

/// Operator-provisioned PEM fixtures (EC P-256, generated offline), shared
/// with `config.rs`'s startup-refusal tests so both read the same certificates.
#[cfg(test)]
pub(crate) mod test_certs {
    /// EC P-256, `CN=ravel-fragment`, SAN `DNS:ravel-fragment`,
    /// `extendedKeyUsage = serverAuth` only: what an operator provisioned
    /// against the pre-#1690 documentation, which specified `serverAuth`
    /// alone.
    pub(crate) const SERVER_AUTH_ONLY_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIB0DCCAXagAwIBAgIUfD0JVWSdmFr5kCGGVy7tRm4rDEMwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MjAwODEw
MzlaGA8yMTI2MDgyNzA4MTAzOVowGTEXMBUGA1UEAwwOcmF2ZWwtZnJhZ21lbnQw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQj9etZSHDXnIEFVrpeqjNFV7+MsH8B
h7ucyn+gB5XOJ8uetqnu9gM8XCaauJ8NeKUsdsT2V9sT7j/0GgefQcIAo4GRMIGO
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUF
BwMBMBkGA1UdEQQSMBCCDnJhdmVsLWZyYWdtZW50MB0GA1UdDgQWBBQOdIqLTp9R
8LI/+w0JfJPCQAJd4jAfBgNVHSMEGDAWgBTu2DqCIwSbcdJPSi4phml2HpZcTjAK
BggqhkjOPQQDAgNIADBFAiEAtQvkkv5YcyzWbhHcrj8fYMPZns4/YIOgwHM1Fvx9
QM8CIDvcoqsE3As/yi4jTEYrVudU0FTugX8HUxxD+UGFKCoI
-----END CERTIFICATE-----
";

    /// The same shape with `extendedKeyUsage = serverAuth, clientAuth`: the
    /// certificate #1690 requires, and the one `distrib.rs`'s TLS tests use.
    pub(crate) const BOTH_USAGES_PEM: &str = "-----BEGIN CERTIFICATE-----
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

    /// A CA certificate with no `extendedKeyUsage` extension at all, which RFC
    /// 5280 leaves unconstrained.
    pub(crate) const NO_EKU_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBmTCCAT+gAwIBAgIUM1B9Y0dbjBhD+jO+VFn812oYU8YwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MjAwODEw
MzlaGA8yMTI2MDgyNzA4MTAzOVowITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQt
dGVzdC1jYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABPjBU2z8vydXRESZWTPZ
134IW5IulGuRNCGLM1EU7Xb2Y2Fz/xySOeQvUFwsQuUZPQEloXsSlkasNrl1wgh/
cYWjUzBRMB0GA1UdDgQWBBTu2DqCIwSbcdJPSi4phml2HpZcTjAfBgNVHSMEGDAW
gBTu2DqCIwSbcdJPSi4phml2HpZcTjAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49
BAMCA0gAMEUCIQCLsdlIEagLDN1CCOwcrW/74ym1Vaa/zDdjvVub1ILWvAIgB/pu
0J/dFiX2cexyWQroOm0v47FktsJqK90Be6jo8mY=
-----END CERTIFICATE-----
";
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::test_certs::{BOTH_USAGES_PEM, NO_EKU_PEM, SERVER_AUTH_ONLY_PEM};
    use super::*;

    /// The upgrade hazard: nothing used to parse the certificate, so a
    /// `serverAuth`-only identity started, served inbound fetches, and failed
    /// every outbound dial at the handshake.
    #[test]
    fn server_auth_only_certificate_is_refused() {
        let path = Path::new("/etc/ravel/fragment-tls/tls.crt");
        let err = ensure_client_auth_eku(path, SERVER_AUTH_ONLY_PEM.as_bytes())
            .expect_err("a serverAuth-only fragment certificate must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("/etc/ravel/fragment-tls/tls.crt"),
            "the error names the certificate path: {msg}"
        );
        assert!(
            msg.contains("missing the clientAuth extended key usage"),
            "the error names the missing usage: {msg}"
        );
        assert!(
            msg.contains("it carries: serverAuth"),
            "the error names what the certificate does carry: {msg}"
        );
        assert!(
            msg.contains("extendedKeyUsage = serverAuth, clientAuth"),
            "the error names what to regenerate: {msg}"
        );
    }

    /// The positive control: a certificate with both usages is what #1690
    /// requires, and it must start normally.
    #[test]
    fn certificate_with_both_usages_is_accepted() {
        ensure_client_auth_eku(Path::new("tls.crt"), BOTH_USAGES_PEM.as_bytes())
            .expect("a serverAuth+clientAuth certificate starts normally");
    }

    /// An absent extension constrains nothing, so it is not the hazard and must
    /// not be refused.
    #[test]
    fn certificate_without_the_extension_is_accepted() {
        ensure_client_auth_eku(Path::new("tls.crt"), NO_EKU_PEM.as_bytes())
            .expect("a certificate with no extendedKeyUsage extension is unconstrained");
    }

    /// Both usages are reported, so the refusal message above is reading a real
    /// extension rather than defaulting to one entry.
    #[test]
    fn both_usages_are_read_from_the_extension() {
        let der = CertificateDer::from_pem_slice(BOTH_USAGES_PEM.as_bytes()).expect("PEM");
        let usages = extended_key_usages(der.as_ref())
            .expect("parses")
            .expect("carries an extendedKeyUsage extension");
        assert_eq!(render_usages(&usages), "serverAuth, clientAuth");
    }

    #[test]
    fn server_auth_only_reads_exactly_one_usage() {
        let der = CertificateDer::from_pem_slice(SERVER_AUTH_ONLY_PEM.as_bytes()).expect("PEM");
        let usages = extended_key_usages(der.as_ref())
            .expect("parses")
            .expect("carries an extendedKeyUsage extension");
        assert_eq!(render_usages(&usages), "serverAuth");
    }

    #[test]
    fn non_pem_input_is_refused_by_path() {
        let err = ensure_client_auth_eku(Path::new("/etc/ravel/tls.crt"), b"not a certificate")
            .expect_err("a file with no PEM CERTIFICATE block must refuse startup");
        assert!(
            err.to_string().contains("/etc/ravel/tls.crt"),
            "the error names the file: {err}"
        );
    }
}
