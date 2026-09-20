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

/// `id-ce-extKeyUsage`, the certificate extension listing the purposes a
/// certificate may be used for (RFC 5280 section 4.2.1.12).
const EXT_KEY_USAGE_OID: &str = "2.5.29.37";

/// `id-kp-clientAuth`, the usage a TLS client certificate needs.
const CLIENT_AUTH_OID: &str = "1.3.6.1.5.5.7.3.2";

/// `anyExtendedKeyUsage`: a certificate listing it is usable for every purpose,
/// so it satisfies the `clientAuth` requirement.
const ANY_EXTENDED_KEY_USAGE_OID: &str = "2.5.29.37.0";

/// DER identifier octets for the handful of types this walk names. Everything
/// else is skipped by its length without being interpreted.
const TAG_OCTET_STRING: u8 = 0x04;
const TAG_OBJECT_IDENTIFIER: u8 = 0x06;
const TAG_SEQUENCE: u8 = 0x30;
/// `[3] EXPLICIT Extensions OPTIONAL`, the last TBSCertificate field.
const TAG_EXTENSIONS: u8 = 0xa3;

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
            "could not determine whether --fragment-tls-cert {} carries the clientAuth extended \
             key usage: {e}. Read it yourself with `openssl x509 -in {} -noout -ext \
             extendedKeyUsage`: the fragment listener is mutual TLS (ADR-0071 amendment decision \
             1) and this process presents that certificate as its client identity on every \
             outbound fragment dial, so a certificate without clientAuth fails every dial at the \
             handshake. Startup refuses on an unreadable certificate rather than starting on one \
             that may not be able to dial at all.",
            path.display(),
            path.display()
        )
    })?;
    let Some(usages) = usages else {
        // No extension: unconstrained, so it can be presented as either role.
        return Ok(());
    };
    if usages.iter().any(|usage| {
        usage.as_str() == CLIENT_AUTH_OID || usage.as_str() == ANY_EXTENDED_KEY_USAGE_OID
    }) {
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

/// One DER element: its identifier octet and the contents the length octets
/// delimit.
struct Element<'a> {
    tag: u8,
    contents: &'a [u8],
}

/// Split the first DER element off `bytes`, returning it and what follows.
///
/// Only the tag and the length are interpreted, so a value this walk does not
/// need is stepped over whatever it holds, including nothing at all.
fn split_element(bytes: &[u8]) -> anyhow::Result<(Element<'_>, &[u8])> {
    let [tag, rest @ ..] = bytes else {
        anyhow::bail!("a DER element is missing its identifier octet");
    };
    if tag & 0x1f == 0x1f {
        anyhow::bail!("a DER element uses the high-tag-number form, which X.509 does not");
    }
    let (length, rest) = match rest {
        [first @ 0x00..=0x7f, rest @ ..] => (usize::from(*first), rest),
        [0x80, ..] => anyhow::bail!("a DER element uses indefinite-length encoding"),
        [first, rest @ ..] => {
            let count = usize::from(first & 0x7f);
            let Some((octets, rest)) = rest.split_at_checked(count) else {
                anyhow::bail!("a DER element's long-form length is truncated");
            };
            let mut length = 0usize;
            for octet in octets {
                let Some(shifted) = length.checked_mul(256) else {
                    anyhow::bail!("a DER element declares a length this platform cannot address");
                };
                length = shifted + usize::from(*octet);
            }
            (length, rest)
        }
        [] => anyhow::bail!("a DER element is missing its length octets"),
    };
    let Some((contents, rest)) = rest.split_at_checked(length) else {
        anyhow::bail!("a DER element declares {length} content octets that are not present");
    };
    Ok((
        Element {
            tag: *tag,
            contents,
        },
        rest,
    ))
}

/// Every DER element of a constructed value's contents, in order.
fn elements(contents: &[u8]) -> anyhow::Result<Vec<Element<'_>>> {
    let mut rest = contents;
    let mut found = Vec::new();
    while !rest.is_empty() {
        let (element, tail) = split_element(rest)?;
        found.push(element);
        rest = tail;
    }
    Ok(found)
}

/// The `extendedKeyUsage` OIDs of a DER-encoded X.509 certificate in
/// dotted-decimal form, or `None` when it carries no such extension.
///
/// The walk descends by tag and length to the extensions and decodes nothing
/// else, so a field that is legitimately empty (the subject and issuer
/// distinguished names of the cert-manager certificate in
/// docs/guides/operations/deployment.md are both the empty SEQUENCE) or
/// legitimately absent is stepped over rather than parsed.
fn extended_key_usages(der: &[u8]) -> anyhow::Result<Option<Vec<String>>> {
    let (certificate, _) = split_element(der)?;
    if certificate.tag != TAG_SEQUENCE {
        anyhow::bail!("expected a SEQUENCE (Certificate) at the top level");
    }
    let Some(tbs) = elements(certificate.contents)?.into_iter().next() else {
        anyhow::bail!("expected a SEQUENCE (TBSCertificate) as the first Certificate field");
    };
    if tbs.tag != TAG_SEQUENCE {
        anyhow::bail!("expected a SEQUENCE (TBSCertificate) as the first Certificate field");
    }
    // The version is `[0]` and the two unique identifiers are `[1]` and `[2]`,
    // so the tag is what selects `[3] EXPLICIT Extensions OPTIONAL`.
    let fields = elements(tbs.contents)?;
    let Some(extensions) = fields.iter().find(|field| field.tag == TAG_EXTENSIONS) else {
        return Ok(None);
    };
    let inner = elements(extensions.contents)?;
    let [extensions] = inner.as_slice() else {
        anyhow::bail!("expected one SEQUENCE inside the [3] extensions field");
    };
    if extensions.tag != TAG_SEQUENCE {
        anyhow::bail!("expected a SEQUENCE inside the [3] extensions field");
    }
    for extension in elements(extensions.contents)? {
        if extension.tag != TAG_SEQUENCE {
            continue;
        }
        let fields = elements(extension.contents)?;
        let Some(id) = fields.first() else {
            continue;
        };
        if id.tag != TAG_OBJECT_IDENTIFIER
            || dotted_oid(id.contents).as_deref() != Some(EXT_KEY_USAGE_OID)
        {
            continue;
        }
        // Extension ::= SEQUENCE { extnID, critical BOOLEAN DEFAULT FALSE,
        // extnValue OCTET STRING }; the value is always last.
        let Some(value) = fields.last().filter(|last| last.tag == TAG_OCTET_STRING) else {
            anyhow::bail!("the extendedKeyUsage extension carries no OCTET STRING value");
        };
        let (purposes, _) = split_element(value.contents)?;
        if purposes.tag != TAG_SEQUENCE {
            anyhow::bail!("expected a SEQUENCE of key purpose OIDs in extendedKeyUsage");
        }
        return Ok(Some(
            elements(purposes.contents)?
                .into_iter()
                .filter(|purpose| purpose.tag == TAG_OBJECT_IDENTIFIER)
                // An OID this walk cannot read is reported as unreadable rather
                // than dropped: it must not match clientAuth, and it must not
                // vanish from the refusal that then names what the certificate
                // carries.
                .map(|purpose| {
                    dotted_oid(purpose.contents).unwrap_or_else(|| "<unreadable OID>".to_string())
                })
                .collect(),
        ));
    }
    Ok(None)
}

/// An OID's contents in the dotted-decimal spelling an operator reads in
/// `openssl x509` output, or `None` when they are not a readable OID.
fn dotted_oid(contents: &[u8]) -> Option<String> {
    let mut arcs: Vec<u128> = Vec::new();
    let mut value: u128 = 0;
    let mut partial = false;
    for octet in contents {
        value = value
            .checked_mul(128)?
            .checked_add(u128::from(octet & 0x7f))?;
        if octet & 0x80 != 0 {
            partial = true;
            continue;
        }
        if arcs.is_empty() {
            // The first subidentifier packs the first two arcs: the root arc is
            // 0, 1 or 2, and only the first two roots bound the second arc.
            let (root, second) = match value {
                0..40 => (0, value),
                40..80 => (1, value - 40),
                _ => (2, value - 80),
            };
            arcs.push(root);
            arcs.push(second);
        } else {
            arcs.push(value);
        }
        value = 0;
        partial = false;
    }
    if partial || arcs.is_empty() {
        return None;
    }
    Some(
        arcs.iter()
            .map(u128::to_string)
            .collect::<Vec<_>>()
            .join("."),
    )
}

/// The usages an operator would recognise in an error, named where RFC 5280
/// names them and dotted otherwise.
fn render_usages(usages: &[String]) -> String {
    if usages.is_empty() {
        return "no key purposes".to_string();
    }
    usages
        .iter()
        .map(|usage| match usage.as_str() {
            "1.3.6.1.5.5.7.3.1" => "serverAuth",
            "1.3.6.1.5.5.7.3.2" => "clientAuth",
            "1.3.6.1.5.5.7.3.3" => "codeSigning",
            "1.3.6.1.5.5.7.3.4" => "emailProtection",
            "1.3.6.1.5.5.7.3.8" => "timeStamping",
            "1.3.6.1.5.5.7.3.9" => "OCSPSigning",
            other => other,
        })
        .collect::<Vec<_>>()
        .join(", ")
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

    /// `extendedKeyUsage = serverAuth, clientAuth` on a certificate whose
    /// subject and issuer distinguished names are both the empty SEQUENCE.
    /// The cert-manager `Certificate` in docs/guides/operations/deployment.md
    /// asks for exactly this shape (it sets neither `commonName` nor
    /// `subject`), so a check that cannot read it refuses the deployment this
    /// repository documents.
    pub(crate) const EMPTY_SUBJECT_BOTH_USAGES_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBlDCCATmgAwIBAgIUF5j4iNe9M3khdz1220WLLbWfTxwwCgYIKoZIzj0EAwIw
ADAgFw0yNjA5MjAwOTIyMDlaGA8yMTI2MDgyNzA5MjIwOVowADBZMBMGByqGSM49
AgEGCCqGSM49AwEHA0IABFU7D6aS6J3+U3QIwbcUMW6ghlpMXsgkI1hcLwh4qWgB
I6gXyDCgG05L/w82fjUjtAy1aDbub94z0titzlCGS5ujgY4wgYswHQYDVR0OBBYE
FJEAogTc71kojY5gmelfxmdSfKRMMB8GA1UdIwQYMBaAFJEAogTc71kojY5gmelf
xmdSfKRMMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsG
AQUFBwMCMBkGA1UdEQQSMBCCDnJhdmVsLWZyYWdtZW50MAoGCCqGSM49BAMCA0kA
MEYCIQCwbjCYe3nazTQxb4xevEU6ExCK0t5KPKMJIGmBmiruSgIhAJanbT2hfxjA
wnccV4duK2Ul5SiGWfSweSZZ/5nPIvON
-----END CERTIFICATE-----
";

    /// The same empty-subject shape carrying `serverAuth` alone: an empty
    /// distinguished name is not itself a reason to accept a certificate, so
    /// this one must still refuse.
    pub(crate) const EMPTY_SUBJECT_SERVER_AUTH_ONLY_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBiTCCAS+gAwIBAgIURFZnfQN8mbQyVHbi1ykXgd793+kwCgYIKoZIzj0EAwIw
ADAgFw0yNjA5MjAwOTIyMjNaGA8yMTI2MDgyNzA5MjIyM1owADBZMBMGByqGSM49
AgEGCCqGSM49AwEHA0IABD1raXcGhZOmFCsEs+WOFDTggxuXgXmWUQKaCi+WGGKm
L0mCDuwGy37ITNwjhpG0beD5kNuGnIYN+zCcaItsUsSjgYQwgYEwHQYDVR0OBBYE
FLJz8FA1KYu00nomCXtU0Ay9vzoGMB8GA1UdIwQYMBaAFLJz8FA1KYu00nomCXtU
0Ay9vzoGMA8GA1UdEwEB/wQFMAMBAf8wEwYDVR0lBAwwCgYIKwYBBQUHAwEwGQYD
VR0RBBIwEIIOcmF2ZWwtZnJhZ21lbnQwCgYIKoZIzj0EAwIDSAAwRQIhAMb2dKje
ROd2O6b/lRD9jFN4vypkSMPfB1OqPQHGx1dWAiAZ2PuE9cEdzXDCAyKuPq2J12tf
bmCMSVSJlWpbCBh0hQ==
-----END CERTIFICATE-----
";

    /// `extendedKeyUsage = critical, serverAuth, clientAuth`: the extension
    /// SEQUENCE carries a `critical BOOLEAN` between the OID and the value,
    /// which is the DEFAULT-FALSE field cert-manager's `isCA` issuers and
    /// several public CAs emit.
    pub(crate) const CRITICAL_EKU_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIByTCCAW6gAwIBAgIUVbpswwccdaiLWDHYa/7b6nBJlYwwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOcmF2ZWwtZnJhZ21lbnQwIBcNMjYwOTIwMDkyMjIzWhgPMjEy
NjA4MjcwOTIyMjNaMBkxFzAVBgNVBAMMDnJhdmVsLWZyYWdtZW50MFkwEwYHKoZI
zj0CAQYIKoZIzj0DAQcDQgAE6v2rlOHWH4Gv7TMnWBYPRvqidjoXYuDC54GtiuE3
V/niPwH6hyQrGVL/ycqvd5sntpN997I9rwbIXpGTsYaNiKOBkTCBjjAdBgNVHQ4E
FgQU4RYiX1W1E+o77rjylp4Wiy8WBSwwHwYDVR0jBBgwFoAU4RYiX1W1E+o77rjy
lp4Wiy8WBSwwDwYDVR0TAQH/BAUwAwEB/zAgBgNVHSUBAf8EFjAUBggrBgEFBQcD
AQYIKwYBBQUHAwIwGQYDVR0RBBIwEIIOcmF2ZWwtZnJhZ21lbnQwCgYIKoZIzj0E
AwIDSQAwRgIhAIhdiLeqp6Bj0YXiZ+TnpSfpj3fQSk215RpqpTTWfbfkAiEAr50z
4vfbV91O32Bg3nExwZhpELN5ZAwK0/We8vpR+vU=
-----END CERTIFICATE-----
";

    /// RSA-2048 with a three-RDN subject and a UTCTime `notAfter`, carrying
    /// both usages: the field encodings an EC fixture never exercises.
    pub(crate) const RSA_UTCTIME_BOTH_USAGES_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDlzCCAn+gAwIBAgIUVyF+9/ZKIZtZ7ZQYMaScitUnlT0wDQYJKoZIhvcNAQEL
BQAwPTESMBAGA1UECgwJTk9GaXJlIEFJMQ4wDAYDVQQLDAVSYXZlbDEXMBUGA1UE
AwwOcmF2ZWwtZnJhZ21lbnQwHhcNMjYwOTIwMDkyMjIzWhcNMzYwOTE3MDkyMjIz
WjA9MRIwEAYDVQQKDAlOT0ZpcmUgQUkxDjAMBgNVBAsMBVJhdmVsMRcwFQYDVQQD
DA5yYXZlbC1mcmFnbWVudDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ANX0cznimJoYj1Sf/adrbk+tvgjKgZYlKJIMVdia6u1GpXdmOWaenPWUbh+i7kTX
GZGMOtqTANtGN1QqpHcO9A8xmCvNSBAHrlDD0sD2BFZKlXgg4LkAohJk2ijhzL8o
bVAVyHF+Lg6O0uBsZC85Ttk5AUPc0G1tSrUTaiyTPVK5xkdNM9ooXNOUY4ntCs4q
acgBm42XPtmjFpEfuGHO7P+XczHC1Cpev8pgX5ihLA5Oq0CTuugWk9jKcFan2h2y
nG58PXb+1oys0C6QVCbaTsIVurU3ngqhqm/uU0XDoUsfYFDiTUYfR1u8xNIhflRG
OXZ/HOyTUUjEblkT+/5D/q8CAwEAAaOBjjCBizAdBgNVHQ4EFgQUptxKUvvRujQq
ajQIer6bDeUmSYswHwYDVR0jBBgwFoAUptxKUvvRujQqajQIer6bDeUmSYswDwYD
VR0TAQH/BAUwAwEB/zAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwGQYD
VR0RBBIwEIIOcmF2ZWwtZnJhZ21lbnQwDQYJKoZIhvcNAQELBQADggEBABwBfwpM
I1+XejMFar0OEzIcjXvQi6r3I4APt3Zd/XLQURN1XjusfB1/1SKVsmermHmo+Zn+
jLeOLEzC3akXP0bJmuqKczkOKph8/DmArcXs71Xl0NKI4hjcxFtN/EXmgbXzsBVH
w8rga1T2fHQCx4G43lKjMpYBw2lm0gCxmuf7uD3SlkoJfE9y0YnVEnAzh0FW8tAT
gpGp+nEQ017SM/w4NnrNq6LEOqb2onO8A4+7G6TTyjawcm8pYSln28B0ElaUnLiW
6xnKu6wiGld3m1BI+V+gx2Vf6c/TZtxfEZqyXsgTlIWXZNaI+fN4Ces7jPD86WoB
S38zc9lo/Ng0ve0=
-----END CERTIFICATE-----
";
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::test_certs::{
        BOTH_USAGES_PEM, CRITICAL_EKU_PEM, EMPTY_SUBJECT_BOTH_USAGES_PEM,
        EMPTY_SUBJECT_SERVER_AUTH_ONLY_PEM, NO_EKU_PEM, RSA_UTCTIME_BOTH_USAGES_PEM,
        SERVER_AUTH_ONLY_PEM,
    };
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

    /// The cert-manager `Certificate` in docs/guides/operations/deployment.md
    /// requests neither `commonName` nor `subject`, so its subject and issuer
    /// are both the empty SEQUENCE. The check must read past them to the
    /// extensions.
    #[test]
    fn certificate_with_an_empty_subject_is_accepted() {
        ensure_client_auth_eku(
            Path::new("tls.crt"),
            EMPTY_SUBJECT_BOTH_USAGES_PEM.as_bytes(),
        )
        .expect("an empty-subject certificate carrying clientAuth starts normally");
    }

    /// An empty distinguished name is not a reason to accept: the same shape
    /// without `clientAuth` still refuses, with the message that names the
    /// missing usage.
    #[test]
    fn empty_subject_server_auth_only_certificate_is_refused() {
        let err = ensure_client_auth_eku(
            Path::new("/etc/ravel/fragment-tls/tls.crt"),
            EMPTY_SUBJECT_SERVER_AUTH_ONLY_PEM.as_bytes(),
        )
        .expect_err("an empty subject does not excuse a missing clientAuth usage");
        let msg = err.to_string();
        assert!(
            msg.contains("missing the clientAuth extended key usage"),
            "the error names the missing usage: {msg}"
        );
        assert!(
            msg.contains("it carries: serverAuth"),
            "the error names what the certificate does carry: {msg}"
        );
    }

    /// A `critical` BOOLEAN sits between the extension OID and its value, so
    /// the value is not the second field of the extension SEQUENCE.
    #[test]
    fn certificate_with_a_critical_extension_is_accepted() {
        ensure_client_auth_eku(Path::new("tls.crt"), CRITICAL_EKU_PEM.as_bytes())
            .expect("a critical extendedKeyUsage listing clientAuth starts normally");
    }

    /// A different key type, a three-RDN subject, and a UTCTime `notAfter`:
    /// every TBSCertificate field the check skips changes encoding here.
    #[test]
    fn rsa_certificate_with_a_utctime_expiry_is_accepted() {
        ensure_client_auth_eku(Path::new("tls.crt"), RSA_UTCTIME_BOTH_USAGES_PEM.as_bytes())
            .expect("an RSA certificate carrying clientAuth starts normally");
    }

    #[test]
    fn empty_subject_usages_are_read_from_the_extension() {
        let der =
            CertificateDer::from_pem_slice(EMPTY_SUBJECT_BOTH_USAGES_PEM.as_bytes()).expect("PEM");
        let usages = extended_key_usages(der.as_ref())
            .expect("parses")
            .expect("carries an extendedKeyUsage extension");
        assert_eq!(render_usages(&usages), "serverAuth, clientAuth");
    }

    #[test]
    fn critical_extension_usages_are_read_from_the_extension() {
        let der = CertificateDer::from_pem_slice(CRITICAL_EKU_PEM.as_bytes()).expect("PEM");
        let usages = extended_key_usages(der.as_ref())
            .expect("parses")
            .expect("carries an extendedKeyUsage extension");
        assert_eq!(render_usages(&usages), "serverAuth, clientAuth");
    }

    /// A certificate this process cannot read at all is not silently accepted:
    /// the walk cannot tell whether it carries `clientAuth`, so startup refuses
    /// and the message says so and how to check.
    #[test]
    fn undeterminable_certificate_is_refused_with_a_way_to_check() {
        // A CERTIFICATE block whose DER is a SEQUENCE header declaring 256
        // content octets that are not there.
        let truncated = "-----BEGIN CERTIFICATE-----\nMIIBAA==\n-----END CERTIFICATE-----\n";
        let err = ensure_client_auth_eku(
            Path::new("/etc/ravel/fragment-tls/tls.crt"),
            truncated.as_bytes(),
        )
        .expect_err("a certificate whose usages cannot be determined must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains(
                "could not determine whether --fragment-tls-cert \
                          /etc/ravel/fragment-tls/tls.crt carries the clientAuth extended key \
                          usage"
            ),
            "the error names the path and what could not be determined: {msg}"
        );
        assert!(
            msg.contains(
                "openssl x509 -in /etc/ravel/fragment-tls/tls.crt -noout -ext extendedKeyUsage"
            ),
            "the error tells the operator how to read the extension: {msg}"
        );
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
