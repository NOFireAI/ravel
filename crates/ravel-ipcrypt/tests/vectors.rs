//! Suite A (VECTORS): pins every `ipcrypt-deterministic` and `ipcrypt-pfx`
//! vector from the draft's own test_vectors.json (see
//! `tests/data/README.md` for provenance), both directions. This exercises
//! [`ravel_ipcrypt::render::encrypt_address`] /
//! [`ravel_ipcrypt::render::decrypt_address`] directly against raw key
//! material, independent of this crate's own key derivation, so a
//! regression in [`ravel_ipcrypt::keys`] cannot hide a rendering bug and
//! vice versa.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_ipcrypt::policy::Variant;
use ravel_ipcrypt::render::{RenderOutcome, decrypt_address, encrypt_address};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Vector {
    variant: String,
    key: String,
    ip: String,
    encrypted_ip: String,
}

fn load_vectors() -> Vec<Vector> {
    let text = include_str!("data/ipcrypt_test_vectors.json");
    serde_json::from_str(text).expect("vector file parses as JSON")
}

#[test]
fn every_vector_encrypts_and_decrypts() {
    let vectors = load_vectors();
    assert_eq!(
        vectors.len(),
        21,
        "expected 5 deterministic + 16 pfx vectors"
    );

    let mut deterministic_count = 0;
    let mut pfx_count = 0;

    for v in &vectors {
        let key_bytes = hex::decode(&v.key).expect("vector key is valid hex");
        let variant = match v.variant.as_str() {
            "ipcrypt-deterministic" => {
                deterministic_count += 1;
                Variant::Deterministic
            }
            "ipcrypt-pfx" => {
                pfx_count += 1;
                Variant::Prefix
            }
            other => panic!("unexpected variant in vector file: {other}"),
        };

        let (encrypted, outcome) =
            encrypt_address(variant, &key_bytes, &v.ip).expect("encrypt succeeds");
        assert_eq!(
            encrypted, v.encrypted_ip,
            "encrypt mismatch for ip={} key={}",
            v.ip, v.key
        );
        assert_eq!(
            outcome,
            RenderOutcome::Encrypted,
            "expected every vector's address to parse, got Unparsed for ip={}",
            v.ip
        );

        let (decrypted, outcome) =
            decrypt_address(variant, &key_bytes, &v.encrypted_ip).expect("decrypt succeeds");
        assert_eq!(
            decrypted, v.ip,
            "decrypt mismatch for encrypted_ip={} key={}",
            v.encrypted_ip, v.key
        );
        assert_eq!(
            outcome,
            RenderOutcome::Encrypted,
            "expected every vector's ciphertext to parse, got Unparsed for encrypted_ip={}",
            v.encrypted_ip
        );
    }

    assert_eq!(deterministic_count, 5);
    assert_eq!(pfx_count, 16);
}
