//! The `ipcrypt_encrypt(ip, epoch)` and `ipcrypt_prefix_encrypt(ip, epoch)`
//! scalar UDFs (ADR-2993 decision 6), and the per-session principal that
//! decides whether a session registers them.
//!
//! A stored address under an IPCrypt policy is ciphertext, so an equality
//! search for a known address needs the same ciphertext on the query side.
//! These functions compute it under the session tenant's key for one variant
//! and epoch. They take no key argument: the key is derived from the
//! deployment key the server holds and the tenant the request authenticated
//! as, so a query can only ever produce its own tenant's ciphertext.
//!
//! Both are `Volatility::Immutable`, so a call over literals folds to a
//! literal at planning time and pushes down like any other string literal.
//! That is safe only because the functions are built per session from that
//! session's tenant ([`SessionPrincipal::ipcrypt_udfs`]); a process-wide
//! instance would fold every tenant's call to the first tenant's ciphertext.
//!
//! A session registers them only when [`SessionPrincipal::registers_ipcrypt`]
//! holds; otherwise they are absent from the session and a call is refused
//! like any unknown function.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use ravel_ipcrypt::policy::Variant;
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::EpochKey;
use ravel_types::TenantHash;

/// The name of the deterministic-variant UDF.
pub const IPCRYPT_ENCRYPT_UDF: &str = "ipcrypt_encrypt";

/// The name of the prefix-preserving-variant UDF.
pub const IPCRYPT_PREFIX_ENCRYPT_UDF: &str = "ipcrypt_prefix_encrypt";

/// Derived keys one function instance keeps. A statement naming more epochs
/// than this still answers; the extra epochs are derived per call instead of
/// cached, so an epoch column cannot grow the cache with the row count.
const MAX_CACHED_EPOCHS: usize = 64;

/// The deployment-level half of the ipcrypt decision: the deployment key, and
/// whether the query audit records statement text in plaintext.
///
/// A plaintext audit would record the literal address beside the statement
/// that encrypts it, so a deployment with one never registers the functions.
/// `Debug` prints whether a key is present, never the key.
#[derive(Clone, Default)]
pub struct IpcryptDeployment {
    key: Option<Arc<[u8; 32]>>,
    plaintext_audit: bool,
}

impl IpcryptDeployment {
    /// `key` is the deployment key, absent when the deployment has none;
    /// `plaintext_audit` is true when the query audit is on and records
    /// statement text unredacted.
    pub fn new(key: Option<Arc<[u8; 32]>>, plaintext_audit: bool) -> Self {
        IpcryptDeployment {
            key,
            plaintext_audit,
        }
    }

    /// Whether a deployment key is present.
    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// Whether the query audit records statement text in plaintext.
    pub fn plaintext_audit(&self) -> bool {
        self.plaintext_audit
    }
}

impl std::fmt::Debug for IpcryptDeployment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpcryptDeployment")
            .field("key", &self.key.as_ref().map(|_| "<redacted>"))
            .field("plaintext_audit", &self.plaintext_audit)
            .finish()
    }
}

/// What the authenticated principal may do in a SQL session beyond plain
/// queries. Callers that cannot know the principal pass the default, which
/// grants nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SqlCapabilities {
    /// The principal holds the `ipcrypt` capability (ADR-2993 decision 6).
    pub ipcrypt: bool,
}

/// Who a session is built for: the tenant every provider in it was built
/// from, the principal's capabilities, and the deployment's ipcrypt setting.
#[derive(Clone, Debug)]
pub struct SessionPrincipal {
    /// The tenant the request authenticated as.
    pub tenant_hash: TenantHash,
    /// The principal holds the `ipcrypt` capability.
    pub ipcrypt: bool,
    /// The deployment key and audit setting.
    pub deployment: IpcryptDeployment,
}

impl SessionPrincipal {
    /// A principal with every capability off and no deployment key.
    pub fn without_capabilities(tenant_hash: TenantHash) -> Self {
        SessionPrincipal {
            tenant_hash,
            ipcrypt: false,
            deployment: IpcryptDeployment::default(),
        }
    }

    /// A principal with `capabilities` under `deployment`.
    pub fn new(
        tenant_hash: TenantHash,
        capabilities: SqlCapabilities,
        deployment: IpcryptDeployment,
    ) -> Self {
        SessionPrincipal {
            tenant_hash,
            ipcrypt: capabilities.ipcrypt,
            deployment,
        }
    }

    /// The session registers the ipcrypt functions only when the principal
    /// holds the capability, a deployment key is present, and the query audit
    /// does not record statement text in plaintext.
    pub fn registers_ipcrypt(&self) -> bool {
        self.ipcrypt && self.deployment.key.is_some() && !self.deployment.plaintext_audit
    }

    /// The two ipcrypt UDFs for this session's tenant, or `None` when
    /// [`Self::registers_ipcrypt`] does not hold.
    pub(crate) fn ipcrypt_udfs(&self) -> Option<[ScalarUDF; 2]> {
        if !self.registers_ipcrypt() {
            return None;
        }
        let key = self.deployment.key.as_ref()?;
        Some([
            ScalarUDF::new_from_impl(IpcryptEncryptUdf::new(
                Arc::clone(key),
                self.tenant_hash,
                Variant::Deterministic,
            )),
            ScalarUDF::new_from_impl(IpcryptEncryptUdf::new(
                Arc::clone(key),
                self.tenant_hash,
                Variant::Prefix,
            )),
        ])
    }
}

/// One variant's encrypt function, bound to one tenant.
///
/// Equality and hashing are by instance identity: two instances may hold
/// different tenants or keys, so nothing short of identity may let the
/// optimizer treat their calls as the same expression.
struct IpcryptEncryptUdf {
    signature: Signature,
    variant: Variant,
    tenant_hash: TenantHash,
    deployment_key: Arc<[u8; 32]>,
    keys: Mutex<HashMap<u32, Arc<EpochKey>>>,
}

impl IpcryptEncryptUdf {
    fn new(deployment_key: Arc<[u8; 32]>, tenant_hash: TenantHash, variant: Variant) -> Self {
        IpcryptEncryptUdf {
            signature: Signature::user_defined(Volatility::Immutable),
            variant,
            tenant_hash,
            deployment_key,
            keys: Mutex::new(HashMap::new()),
        }
    }

    fn fn_name(&self) -> &'static str {
        match self.variant {
            Variant::Deterministic => IPCRYPT_ENCRYPT_UDF,
            Variant::Prefix => IPCRYPT_PREFIX_ENCRYPT_UDF,
        }
    }

    /// The epoch as the key schedule takes it. Epochs start at 1, so zero and
    /// negative values are refused, as is a value past `u32::MAX`.
    fn epoch(&self, value: i64) -> DFResult<u32> {
        if value <= 0 {
            return Err(DataFusionError::Execution(format!(
                "{}: epoch must be positive, got {value}",
                self.fn_name()
            )));
        }
        u32::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!(
                "{}: epoch {value} is out of range (at most {})",
                self.fn_name(),
                u32::MAX
            ))
        })
    }

    fn key_for(&self, epoch: u32) -> DFResult<Arc<EpochKey>> {
        let mut keys = self.keys.lock().map_err(|_| {
            DataFusionError::Execution(format!("{}: key cache poisoned", self.fn_name()))
        })?;
        if let Some(key) = keys.get(&epoch) {
            return Ok(Arc::clone(key));
        }
        let key = EpochKey::new(&self.deployment_key, &self.tenant_hash, self.variant, epoch)
            .map(Arc::new)
            .map_err(|err| DataFusionError::Execution(format!("{}: {err}", self.fn_name())))?;
        if keys.len() < MAX_CACHED_EPOCHS {
            keys.insert(epoch, Arc::clone(&key));
        }
        Ok(key)
    }

    /// The ciphertext for `ip` at `epoch`, or `None` when `ip` does not parse
    /// as an address.
    fn encrypt_one(&self, ip: &str, epoch: i64) -> DFResult<Option<String>> {
        let key = self.key_for(self.epoch(epoch)?)?;
        let (rendered, outcome) = key.encrypt(ip);
        Ok(match outcome {
            RenderOutcome::Encrypted => Some(rendered),
            RenderOutcome::Unparsed => None,
        })
    }

    fn encrypt_arrays(&self, ips: &ArrayRef, epochs: &ArrayRef) -> DFResult<ArrayRef> {
        let ips = ips.as_string_opt::<i32>().ok_or_else(|| {
            DataFusionError::Execution(format!("{}: address must be Utf8", self.fn_name()))
        })?;
        let epochs = epochs.as_primitive_opt::<Int64Type>().ok_or_else(|| {
            DataFusionError::Execution(format!("{}: epoch must be Int64", self.fn_name()))
        })?;
        let mut out = StringBuilder::with_capacity(ips.len(), ips.len() * 16);
        for row in 0..ips.len() {
            if ips.is_null(row) || epochs.is_null(row) {
                out.append_null();
                continue;
            }
            out.append_option(self.encrypt_one(ips.value(row), epochs.value(row))?);
        }
        Ok(Arc::new(out.finish()))
    }
}

impl std::fmt::Debug for IpcryptEncryptUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpcryptEncryptUdf")
            .field("name", &self.fn_name())
            .field("tenant_hash", &self.tenant_hash.to_hex())
            .field("deployment_key", &"<redacted>")
            .finish()
    }
}

impl PartialEq for IpcryptEncryptUdf {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for IpcryptEncryptUdf {}

impl std::hash::Hash for IpcryptEncryptUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(self, state);
    }
}

/// The string argument's value, `None` for NULL.
fn scalar_str(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => v.as_deref(),
        _ => None,
    }
}

impl ScalarUDFImpl for IpcryptEncryptUdf {
    fn name(&self) -> &str {
        self.fn_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    /// The address coerces to `Utf8` and the epoch to `Int64`; anything that
    /// cannot is a planning error.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [ip, epoch] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "{}() expects 2 arguments (ip, epoch), got {}",
                self.fn_name(),
                arg_types.len()
            )));
        };
        let ip_ok = match ip {
            DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
            DataType::Dictionary(_, value) => matches!(
                **value,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ),
            _ => false,
        };
        if !ip_ok {
            return Err(DataFusionError::Plan(format!(
                "{}() takes a string address, not {ip}",
                self.fn_name()
            )));
        }
        if !(epoch.is_integer() || *epoch == DataType::Null) {
            return Err(DataFusionError::Plan(format!(
                "{}() takes an integer epoch, not {epoch}",
                self.fn_name()
            )));
        }
        Ok(vec![DataType::Utf8, DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [ip, epoch] = args.args.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "{}() expects 2 arguments, got {}",
                self.fn_name(),
                args.args.len()
            )));
        };
        if let (ColumnarValue::Scalar(ip), ColumnarValue::Scalar(epoch)) = (ip, epoch) {
            let epoch = match epoch {
                ScalarValue::Int64(v) => *v,
                ScalarValue::Null => None,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "{}: epoch must be Int64, got {}",
                        self.fn_name(),
                        other.data_type()
                    )));
                }
            };
            let out = match (scalar_str(ip), epoch) {
                (Some(ip), Some(epoch)) => self.encrypt_one(ip, epoch)?,
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
        }
        let rows = args.number_rows;
        let ips = ip.to_array(rows)?;
        let epochs = epoch.to_array(rows)?;
        Ok(ColumnarValue::Array(self.encrypt_arrays(&ips, &epochs)?))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [7; 32];

    fn principal(ipcrypt: bool, key: bool, plaintext_audit: bool) -> SessionPrincipal {
        SessionPrincipal::new(
            TenantHash([1; 16]),
            SqlCapabilities { ipcrypt },
            IpcryptDeployment::new(key.then(|| Arc::new(KEY)), plaintext_audit),
        )
    }

    fn udf(variant: Variant) -> IpcryptEncryptUdf {
        IpcryptEncryptUdf::new(Arc::new(KEY), TenantHash([1; 16]), variant)
    }

    #[test]
    fn registration_needs_capability_key_and_no_plaintext_audit() {
        assert!(principal(true, true, false).registers_ipcrypt());
        assert!(!principal(false, true, false).registers_ipcrypt());
        assert!(!principal(true, false, false).registers_ipcrypt());
        assert!(!principal(true, true, true).registers_ipcrypt());
        assert!(principal(false, true, false).ipcrypt_udfs().is_none());
        let names = principal(true, true, false)
            .ipcrypt_udfs()
            .expect("registers")
            .map(|udf| udf.name().to_owned());
        assert_eq!(names, [IPCRYPT_ENCRYPT_UDF, IPCRYPT_PREFIX_ENCRYPT_UDF]);
    }

    #[test]
    fn encrypts_like_the_epoch_key_and_keeps_ports() {
        let f = udf(Variant::Deterministic);
        let key =
            EpochKey::new(&KEY, &TenantHash([1; 16]), Variant::Deterministic, 3).expect("key");
        let expected = key.encrypt("192.0.2.1").0;
        let got = f.encrypt_one("192.0.2.1", 3).expect("ok").expect("parses");
        assert_eq!(got, expected);
        assert_ne!(got, "192.0.2.1");
        // The deterministic ciphertext of an IPv4 address renders as IPv6
        // text, so its port comes back in the bracketed form.
        let with_port = f
            .encrypt_one("192.0.2.1:8080", 3)
            .expect("ok")
            .expect("parses");
        assert_eq!(with_port, key.encrypt("192.0.2.1:8080").0);
        assert_eq!(with_port, format!("[{expected}]:8080"));
        let v6 = f
            .encrypt_one("[2001:db8::1]:443", 3)
            .expect("ok")
            .expect("parses");
        assert!(v6.starts_with('[') && v6.ends_with("]:443"), "{v6}");
    }

    #[test]
    fn unparseable_address_is_null_and_epochs_must_be_positive() {
        let f = udf(Variant::Prefix);
        assert_eq!(f.encrypt_one("not an address", 1).expect("ok"), None);
        for bad in [0, -1, i64::from(u32::MAX) + 1] {
            let err = f.encrypt_one("192.0.2.1", bad).expect_err("refused");
            let text = err.to_string();
            assert!(text.contains("epoch"), "{text}");
            assert!(!text.contains(&hex::encode(KEY)), "{text}");
        }
        let large = f.encrypt_one("192.0.2.1", i64::from(u32::MAX)).expect("ok");
        assert!(large.is_some());
    }

    #[test]
    fn array_path_maps_nulls_and_unparsed_to_null() {
        let f = udf(Variant::Deterministic);
        let ips: ArrayRef = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            Some("192.0.2.1"),
            None,
            Some("bogus"),
            Some("192.0.2.1"),
        ]));
        let epochs: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            Some(1),
            Some(1),
            Some(1),
            None,
        ]));
        let out = f.encrypt_arrays(&ips, &epochs).expect("ok");
        let out = out.as_string::<i32>();
        assert!(out.is_valid(0));
        assert!(out.is_null(1) && out.is_null(2) && out.is_null(3));
    }

    #[test]
    fn debug_redacts_the_key() {
        let rendered = format!(
            "{:?} {:?}",
            principal(true, true, false),
            udf(Variant::Deterministic)
        );
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(!rendered.contains("7, 7"), "{rendered}");
    }

    #[test]
    fn each_epoch_caches_its_key_up_to_the_bound() {
        let f = udf(Variant::Deterministic);
        let first = f.key_for(5).expect("key");
        let again = f.key_for(5).expect("key");
        assert!(Arc::ptr_eq(&first, &again));
        for epoch in 1..=(MAX_CACHED_EPOCHS as u32 + 10) {
            f.key_for(epoch).expect("key");
        }
        assert_eq!(f.keys.lock().expect("lock").len(), MAX_CACHED_EPOCHS);
    }
}
