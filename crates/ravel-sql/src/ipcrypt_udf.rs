//! The `ipcrypt_encrypt(ip, epoch)` and `ipcrypt_prefix_encrypt(ip, epoch)`
//! scalar UDFs (ADR-2993 decision 6).
//!
//! Ingest stores an address under an IPCrypt rule as ciphertext, so a query
//! cannot compare that column against a plaintext literal. These functions
//! encrypt the literal under the same key ingest used: the deployment key, the
//! calling tenant's hash, the variant and the epoch, through
//! [`EpochKey::new`]. Neither takes a key argument. The key is fixed when the
//! session is built, from the tenant the request authenticated as, so a
//! statement cannot name another tenant's key.
//!
//! Both are [`Volatility::Immutable`], so a call over literals folds to a
//! string literal at planning time and an equality on it reaches the scan as
//! an ordinary pushed-down literal. The folded ciphertext is the calling
//! tenant's: every session builds its own instances ([`SessionIpcrypt::udfs`]).
//!
//! The session builder registers them only when [`SessionIpcrypt::registers`]
//! holds: the principal has the `ipcrypt` capability, a deployment key is
//! configured, and the query audit is not recording statement text verbatim
//! (the plaintext address in the statement would otherwise land in the audit
//! log).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use ravel_ipcrypt::policy::Variant;
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::EpochKey;
use ravel_types::TenantHash;

/// The deterministic-variant function's name.
pub const IPCRYPT_ENCRYPT_UDF: &str = "ipcrypt_encrypt";

/// The prefix-preserving-variant function's name.
pub const IPCRYPT_PREFIX_ENCRYPT_UDF: &str = "ipcrypt_prefix_encrypt";

/// The most epoch keys one function instance keeps. A statement whose epoch
/// argument is a column can name many epochs; past this many, a key is
/// derived for the row and not kept.
const MAX_CACHED_EPOCHS: usize = 64;

/// What [`crate::build_session_with_tables`] needs to decide whether a
/// session gets the IPCrypt functions, and to key them when it does.
///
/// The `Default` value registers nothing; it is what a caller with no
/// principal passes ([`Self::disabled`]).
#[derive(Clone, Default)]
pub struct SessionIpcrypt {
    /// The tenant the request authenticated as. The functions derive their
    /// keys from this hash, the same one ingest derives from. `None`
    /// registers nothing.
    pub tenant_hash: Option<TenantHash>,
    /// Whether the principal holds the `ipcrypt` capability.
    pub principal_ipcrypt: bool,
    /// The deployment key (`--deployment-key-file`), or `None` when the
    /// deployment has none.
    pub deployment_key: Option<Arc<[u8; 32]>>,
    /// Whether the query audit is on and records statement text verbatim
    /// (`--audit-text plaintext`). A statement naming these functions carries
    /// a plaintext address, so a session where this is true never registers
    /// them.
    pub audit_plaintext: bool,
}

impl std::fmt::Debug for SessionIpcrypt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionIpcrypt")
            .field("tenant_hash", &self.tenant_hash)
            .field("principal_ipcrypt", &self.principal_ipcrypt)
            .field("deployment_key", &self.deployment_key.is_some())
            .field("audit_plaintext", &self.audit_plaintext)
            .finish()
    }
}

impl SessionIpcrypt {
    /// A value that registers nothing: for a caller that cannot know the
    /// principal, and for a session that only ever plans built-in functions.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Whether a session built with this value registers
    /// [`IPCRYPT_ENCRYPT_UDF`] and [`IPCRYPT_PREFIX_ENCRYPT_UDF`]: the
    /// principal has the capability, a tenant and a deployment key are known,
    /// and the audit is not on under the plaintext text policy.
    pub fn registers(&self) -> bool {
        self.keying().is_some()
    }

    fn keying(&self) -> Option<(TenantHash, &Arc<[u8; 32]>)> {
        if !self.principal_ipcrypt || self.audit_plaintext {
            return None;
        }
        Some((self.tenant_hash?, self.deployment_key.as_ref()?))
    }

    /// Fresh instances of both functions keyed for this session's tenant, or
    /// none when [`Self::registers`] is false. Each call builds new instances
    /// with empty key caches, so no derived key outlives the session.
    pub(crate) fn udfs(&self) -> Vec<ScalarUDF> {
        let Some((tenant_hash, deployment_key)) = self.keying() else {
            return Vec::new();
        };
        [
            (IPCRYPT_ENCRYPT_UDF, Variant::Deterministic),
            (IPCRYPT_PREFIX_ENCRYPT_UDF, Variant::Prefix),
        ]
        .into_iter()
        .map(|(name, variant)| {
            ScalarUDF::new_from_impl(IpcryptEncryptUdf {
                name,
                variant,
                signature: Signature::user_defined(Volatility::Immutable),
                tenant_hash,
                deployment_key: Arc::clone(deployment_key),
                keys: Mutex::new(HashMap::new()),
            })
        })
        .collect()
    }
}

/// One of the two functions, keyed for one tenant. The derived keys are cached
/// per epoch for the life of the instance, which is one session.
struct IpcryptEncryptUdf {
    name: &'static str,
    variant: Variant,
    signature: Signature,
    tenant_hash: TenantHash,
    deployment_key: Arc<[u8; 32]>,
    keys: Mutex<HashMap<u32, Arc<EpochKey>>>,
}

impl std::fmt::Debug for IpcryptEncryptUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpcryptEncryptUdf")
            .field("name", &self.name)
            .field("tenant_hash", &self.tenant_hash)
            .finish_non_exhaustive()
    }
}

impl PartialEq for IpcryptEncryptUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.tenant_hash == other.tenant_hash
            && self.deployment_key == other.deployment_key
    }
}

impl Eq for IpcryptEncryptUdf {}

impl Hash for IpcryptEncryptUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.tenant_hash.hash(state);
    }
}

impl IpcryptEncryptUdf {
    /// The key for `epoch`, derived on first use.
    fn key(&self, epoch: u32) -> DFResult<Arc<EpochKey>> {
        let mut keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(key) = keys.get(&epoch) {
            return Ok(Arc::clone(key));
        }
        let key = EpochKey::new(
            &self.deployment_key,
            &self.tenant_hash,
            self.variant,
            epoch,
        )
        .map(Arc::new)
        .map_err(|_| {
            DataFusionError::Execution(format!(
                "{}(): no usable key can be derived for epoch {epoch}",
                self.name
            ))
        })?;
        if keys.len() < MAX_CACHED_EPOCHS {
            keys.insert(epoch, Arc::clone(&key));
        }
        Ok(key)
    }

    /// One row: `None` for a NULL argument or an address that does not parse.
    /// The epoch is checked before the address, so an epoch out of range is
    /// an error whatever the address is.
    fn encrypt(&self, ip: Option<&str>, epoch: Option<i64>) -> DFResult<Option<String>> {
        let Some(epoch) = epoch else {
            return Ok(None);
        };
        let epoch = u32::try_from(epoch)
            .ok()
            .filter(|epoch| *epoch > 0)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}(): epoch must be between 1 and {}, got {epoch}",
                    self.name,
                    u32::MAX
                ))
            })?;
        let Some(ip) = ip else {
            return Ok(None);
        };
        let (out, outcome) = self.key(epoch)?.encrypt(ip);
        Ok(match outcome {
            RenderOutcome::Encrypted => Some(out),
            RenderOutcome::Unparsed => None,
        })
    }
}

impl ScalarUDFImpl for IpcryptEncryptUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    /// The address coerces to `Utf8` from any string type (a declared `Str`
    /// column's dictionary included) or NULL; the epoch to `Int64` from any
    /// integer type or NULL. Anything else is a planning error.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [ip, epoch] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "{}() expects 2 arguments (ip, epoch), got {}",
                self.name,
                arg_types.len()
            )));
        };
        if !is_string_like(ip) {
            return Err(DataFusionError::Plan(format!(
                "{}() first argument must be a string, got {ip}",
                self.name
            )));
        }
        if !(epoch.is_integer() || *epoch == DataType::Null) {
            return Err(DataFusionError::Plan(format!(
                "{}() second argument (the epoch) must be an integer, got {epoch}",
                self.name
            )));
        }
        Ok(vec![DataType::Utf8, DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [ip, epoch] = args.args.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "{}() expects 2 arguments, got {}",
                self.name,
                args.args.len()
            )));
        };
        if let (ColumnarValue::Scalar(ip), ColumnarValue::Scalar(epoch)) = (ip, epoch) {
            let out = self.encrypt(scalar_str(ip, self.name)?, scalar_i64(epoch, self.name)?)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
        }
        let rows = args.number_rows;
        let ips = ip.to_array(rows)?;
        let epochs = epoch.to_array(rows)?;
        let ips = downcast::<StringArray>(&ips, self.name, "first")?;
        let epochs = downcast::<Int64Array>(&epochs, self.name, "second")?;
        let mut out = StringBuilder::with_capacity(rows, rows * 16);
        for row in 0..rows {
            let ip = (!ips.is_null(row)).then(|| ips.value(row));
            let epoch = (!epochs.is_null(row)).then(|| epochs.value(row));
            out.append_option(self.encrypt(ip, epoch)?);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn is_string_like(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => true,
        DataType::Dictionary(_, value) => is_string_like(value),
        _ => false,
    }
}

fn scalar_str<'a>(value: &'a ScalarValue, name: &str) -> DFResult<Option<&'a str>> {
    match value {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
            Ok(s.as_deref())
        }
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{name}() first argument must be a string, got {}",
            other.data_type()
        ))),
    }
}

fn scalar_i64(value: &ScalarValue, name: &str) -> DFResult<Option<i64>> {
    match value {
        ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{name}() second argument must be an integer, got {}",
            other.data_type()
        ))),
    }
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, name: &str, which: &str) -> DFResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{name}() {which} argument has type {}, not the coerced type",
            array.data_type()
        ))
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [7u8; 32];

    fn session(tenant: u8) -> SessionIpcrypt {
        SessionIpcrypt {
            tenant_hash: Some(TenantHash([tenant; 16])),
            principal_ipcrypt: true,
            deployment_key: Some(Arc::new(KEY)),
            audit_plaintext: false,
        }
    }

    fn udf(session: &SessionIpcrypt, name: &str) -> IpcryptEncryptUdf {
        let (tenant_hash, deployment_key) = session.keying().expect("registers");
        IpcryptEncryptUdf {
            name: if name == IPCRYPT_ENCRYPT_UDF {
                IPCRYPT_ENCRYPT_UDF
            } else {
                IPCRYPT_PREFIX_ENCRYPT_UDF
            },
            variant: if name == IPCRYPT_ENCRYPT_UDF {
                Variant::Deterministic
            } else {
                Variant::Prefix
            },
            signature: Signature::user_defined(Volatility::Immutable),
            tenant_hash,
            deployment_key: Arc::clone(deployment_key),
            keys: Mutex::new(HashMap::new()),
        }
    }

    #[test]
    fn registration_needs_every_condition() {
        assert!(session(1).registers());
        assert_eq!(session(1).udfs().len(), 2);
        let mut no_capability = session(1);
        no_capability.principal_ipcrypt = false;
        let mut no_key = session(1);
        no_key.deployment_key = None;
        let mut audited = session(1);
        audited.audit_plaintext = true;
        let mut no_tenant = session(1);
        no_tenant.tenant_hash = None;
        for (label, value) in [
            ("no capability", no_capability),
            ("no deployment key", no_key),
            ("audit under plaintext", audited),
            ("no tenant", no_tenant),
            ("disabled", SessionIpcrypt::disabled()),
        ] {
            assert!(!value.registers(), "{label} must not register");
            assert!(value.udfs().is_empty(), "{label} must build no function");
        }
    }

    #[test]
    fn matches_the_epoch_key_ingest_uses() {
        let tenant = TenantHash([1; 16]);
        for (name, variant) in [
            (IPCRYPT_ENCRYPT_UDF, Variant::Deterministic),
            (IPCRYPT_PREFIX_ENCRYPT_UDF, Variant::Prefix),
        ] {
            let expected = EpochKey::new(&KEY, &tenant, variant, 3)
                .expect("derives")
                .encrypt("192.0.2.10")
                .0;
            let got = udf(&session(1), name)
                .encrypt(Some("192.0.2.10"), Some(3))
                .expect("encrypts");
            assert_eq!(got.as_deref(), Some(expected.as_str()), "{name}");
            assert_ne!(expected, "192.0.2.10");
        }
    }

    #[test]
    fn port_forms_keep_their_port() {
        let deterministic = udf(&session(1), IPCRYPT_ENCRYPT_UDF);
        let v4 = deterministic
            .encrypt(Some("192.0.2.10:8443"), Some(1))
            .expect("encrypts")
            .expect("parses");
        assert!(v4.ends_with(":8443"), "{v4}");
        let v6 = deterministic
            .encrypt(Some("[2001:db8::1]:443"), Some(1))
            .expect("encrypts")
            .expect("parses");
        assert!(v6.starts_with('[') && v6.ends_with("]:443"), "{v6}");
    }

    #[test]
    fn null_and_unparsed_inputs_are_null() {
        let deterministic = udf(&session(1), IPCRYPT_ENCRYPT_UDF);
        assert_eq!(deterministic.encrypt(None, Some(1)).expect("null ip"), None);
        assert_eq!(
            deterministic
                .encrypt(Some("192.0.2.10"), None)
                .expect("null epoch"),
            None
        );
        assert_eq!(
            deterministic
                .encrypt(Some("not an address"), Some(1))
                .expect("unparsed"),
            None
        );
    }

    #[test]
    fn an_epoch_out_of_range_is_an_error_naming_no_address() {
        let deterministic = udf(&session(1), IPCRYPT_ENCRYPT_UDF);
        for epoch in [0, -1, i64::from(u32::MAX) + 1] {
            let err = deterministic
                .encrypt(Some("192.0.2.10"), Some(epoch))
                .expect_err("refused")
                .to_string();
            assert!(err.contains("epoch must be between 1 and"), "{err}");
            assert!(!err.contains("192.0.2.10"), "{err}");
        }
        // The epoch is checked even when the address is NULL.
        assert!(deterministic.encrypt(None, Some(0)).is_err());
        assert!(
            deterministic
                .encrypt(Some("192.0.2.10"), Some(i64::from(u32::MAX)))
                .expect("the largest epoch is accepted")
                .is_some()
        );
    }

    #[test]
    fn two_tenants_get_different_ciphertext() {
        let a = udf(&session(1), IPCRYPT_ENCRYPT_UDF)
            .encrypt(Some("192.0.2.10"), Some(1))
            .expect("encrypts");
        let b = udf(&session(2), IPCRYPT_ENCRYPT_UDF)
            .encrypt(Some("192.0.2.10"), Some(1))
            .expect("encrypts");
        assert_ne!(a, b);
    }

    #[test]
    fn debug_prints_no_key_byte() {
        let text = format!(
            "{:?} {:?}",
            session(1),
            udf(&session(1), IPCRYPT_ENCRYPT_UDF)
        );
        assert!(!text.contains("[7, 7"), "{text}");
    }
}
