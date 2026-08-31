//! Store cost profile: the deployment's object-store prices, in one place
//! (ADR-0996 decision 1).
//!
//! The reference deployment is intra-region, where transfer is free and the
//! bill is requests only. Under that shape a column-range GET spends a billed
//! request to save bytes that cost nothing, so the read path's economics
//! depend on prices that are a **deployment property, not a constant**: they
//! belong in configuration, read once, stamped into every report that carries
//! a request or modeled-cost figure.
//!
//! # Integer nanodollars, never floats
//!
//! Prices are exact decimal contract figures ($5.00 per million requests, not
//! a measured quantity), and the repo's float-comparison discipline exists
//! because floats are inexact. One nanodollar is 1e-9 USD, so $5/M requests is
//! `5_000` nanodollars per request and $0.40/M is `400`. Every figure derived
//! from a profile stays in `u64` nanodollars with saturating arithmetic, so a
//! modeled cost can be compared, summed, and pinned in a test exactly.
//!
//! # Layering: prices live here and are never read by the fetch layer
//!
//! ADR-0904's rule is preserved (ADR-0996 decision 1, "Layering"): this module
//! is data. The server config and `ravel-bench` read it; the server derives a
//! byte-denominated exchange rate from the profile's ratio and hands the query
//! engine only byte quantities. Nothing in the fetch path learns a price.

use serde::{Deserialize, Serialize};

use crate::accounting::AccountedOp;

/// Bytes in one GiB, the unit both per-GiB prices are quoted in.
const BYTES_PER_GIB: u128 = 1 << 30;

/// The billing class a store operation falls under. S3 prices requests in two
/// tiers plus a free tier, and the profile carries one price per tier rather
/// than one field per operation (ADR-0996 decision 1): a new operation kind
/// maps to an existing class instead of growing the config surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreOpClass {
    /// PUT/COPY/POST/LIST class, the expensive tier. LIST bills here on S3
    /// despite being a read, which is why a per-op price table would be a
    /// trap: listing costs 12.5 GETs at the reference prices.
    Put,
    /// GET/SELECT class, the cheap tier. HEAD bills here: it is a GET request
    /// that returns no body.
    Get,
    /// DELETE class, priced by its own field. Zero on the reference S3
    /// profile, but a PRICED class rather than a hardcoded free tier: prices
    /// are a deployment property, and an S3-compatible provider that bills
    /// deletes gets a field to say so instead of a silent zero.
    Delete,
}

/// Store operations a cost profile can price. Mirrors
/// `ravel_object_store::instrument::StoreOp` variant for variant so that crate
/// can map its own op into a class, without this dependency-light crate
/// depending on it (the dependency runs the other way; see [`AccountedOp`]'s
/// own note on the same constraint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CostedStoreOp {
    Put,
    Get,
    Head,
    List,
    ListDelimited,
    Delete,
}

impl CostedStoreOp {
    /// Every variant, for exhaustive mapping tests.
    pub const ALL: [CostedStoreOp; 6] = [
        CostedStoreOp::Put,
        CostedStoreOp::Get,
        CostedStoreOp::Head,
        CostedStoreOp::List,
        CostedStoreOp::ListDelimited,
        CostedStoreOp::Delete,
    ];

    /// The billing class this operation bills under on S3-compatible storage.
    ///
    /// The two mappings that are not the obvious ones, and the reason this
    /// function exists rather than a price per op: **LIST is PUT-class** (both
    /// `List` and `ListDelimited`, which are the same `ListObjectsV2` request
    /// with and without a delimiter), and **HEAD is GET-class**.
    pub fn class(self) -> StoreOpClass {
        match self {
            CostedStoreOp::Put => StoreOpClass::Put,
            CostedStoreOp::List | CostedStoreOp::ListDelimited => StoreOpClass::Put,
            CostedStoreOp::Get | CostedStoreOp::Head => StoreOpClass::Get,
            CostedStoreOp::Delete => StoreOpClass::Delete,
        }
    }
}

impl From<AccountedOp> for CostedStoreOp {
    /// Widens a query-path op into the full billed set. [`AccountedOp`] carries
    /// only the three kinds a query can issue; every one of them has an exact
    /// counterpart here.
    fn from(op: AccountedOp) -> Self {
        match op {
            AccountedOp::Get => CostedStoreOp::Get,
            AccountedOp::List => CostedStoreOp::List,
            AccountedOp::Head => CostedStoreOp::Head,
        }
    }
}

/// The active deployment's object-store prices (ADR-0996 decision 1).
///
/// Ships with a reference profile ([`StoreCostProfile::reference`], named
/// [`StoreCostProfile::S3_INTRA_REGION_2026`]) and is overridable from a TOML
/// document ([`StoreCostProfile::from_toml_str`]), so the bench, the server,
/// and any cost-based planner read one artifact.
///
/// Serialization is TOML config only, never an on-object format: this type is
/// not part of any frozen persistent contract and carries no version domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCostProfile {
    /// Profile identity as it appears in a provenance stamp, e.g.
    /// `"s3-intra-region-2026"`. Stamped beside every request or modeled-cost
    /// figure so two reports can be compared only when they priced the same
    /// way.
    pub name: String,
    /// Nanodollars per PUT-class request (PUT/COPY/POST/LIST).
    pub put_class_nanodollars: u64,
    /// Nanodollars per GET-class request (GET/SELECT/HEAD).
    pub get_class_nanodollars: u64,
    /// Nanodollars per DELETE-class request. `0` on the reference S3 profile
    /// (DELETE is free there); a field rather than a constant because other
    /// providers bill it.
    #[serde(default)]
    pub delete_class_nanodollars: u64,
    /// Nanodollars per GiB transferred out. `0` on an intra-region deployment,
    /// which is what makes request-minimal fetching the cost-preferring shape
    /// there.
    pub transfer_nanodollars_per_gib: u64,
    /// Nanodollars per GiB retrieved, for storage classes that bill retrieval
    /// separately from transfer. `0` on the standard class.
    pub retrieval_nanodollars_per_gib: u64,
}

/// Prices of the reference profile, carrying an empty name: a `String` cannot
/// be built in a `const`, and a public constant whose name field is blank
/// would be a profile that cannot legally be stamped. So the prices live here
/// once, and [`StoreCostProfile::reference`] is the only way to obtain the
/// profile, always named.
const S3_INTRA_REGION_2026_PRICES: StoreCostProfile = StoreCostProfile {
    name: String::new(),
    put_class_nanodollars: 5_000,
    get_class_nanodollars: 400,
    delete_class_nanodollars: 0,
    transfer_nanodollars_per_gib: 0,
    retrieval_nanodollars_per_gib: 0,
};

impl StoreCostProfile {
    /// Name of the reference profile, as it appears in a provenance stamp.
    pub const S3_INTRA_REGION_2026: &'static str = "s3-intra-region-2026";

    /// The reference profile: S3 standard, intra-region, 2026 list prices.
    /// PUT-class $5.00 per million requests (`5_000` nanodollars), GET-class
    /// $0.40 per million (`400`), transfer and retrieval free. One PUT costs
    /// 12.5 GETs, the ratio ADR-0996 reasons from.
    pub fn reference() -> StoreCostProfile {
        StoreCostProfile {
            name: StoreCostProfile::S3_INTRA_REGION_2026.to_string(),
            ..S3_INTRA_REGION_2026_PRICES
        }
    }

    /// Parse a profile from a TOML document.
    ///
    /// Unknown fields are refused rather than ignored: a misspelled price key
    /// would otherwise leave the default silently in force, which is exactly
    /// the "a variable that moves results and is absent from provenance"
    /// failure this profile exists to close. Every error is a typed
    /// [`CostProfileError`]; nothing here panics on malformed input.
    pub fn from_toml_str(toml_str: &str) -> Result<StoreCostProfile, CostProfileError> {
        let profile: StoreCostProfile = toml::from_str(toml_str)?;
        if profile.name.trim().is_empty() {
            return Err(CostProfileError::EmptyName);
        }
        Ok(profile)
    }

    /// Render this profile as a TOML document, for a config file or a
    /// provenance stamp that carries the profile verbatim.
    pub fn to_toml_string(&self) -> Result<String, CostProfileError> {
        Ok(toml::to_string(self)?)
    }

    /// Nanodollars per request under `class`.
    pub fn class_nanodollars(&self, class: StoreOpClass) -> u64 {
        match class {
            StoreOpClass::Put => self.put_class_nanodollars,
            StoreOpClass::Get => self.get_class_nanodollars,
            StoreOpClass::Delete => self.delete_class_nanodollars,
        }
    }

    /// Nanodollars for one request of `op`, through [`CostedStoreOp::class`].
    /// Takes anything that converts, so a query-path [`AccountedOp`] prices
    /// without the caller naming the class.
    pub fn op_nanodollars(&self, op: impl Into<CostedStoreOp>) -> u64 {
        self.class_nanodollars(op.into().class())
    }

    /// Modeled cost of a pass, in nanodollars: requests priced per class plus
    /// transferred bytes priced per GiB.
    ///
    /// `transfer_bytes` is bytes, not GiB, because a fractional GiB has no
    /// exact `u64` representation and money here never touches a float: the
    /// per-GiB price is prorated as `bytes * price / 2^30` in `u128` and
    /// truncated toward zero, so the result is reproducible bit for bit and
    /// never rounds a bill up.
    ///
    /// Every step saturates at `u64::MAX`. This is an observability figure, so
    /// a saturated value reads as "at least this much" rather than wrapping to
    /// a small number that would read as cheap.
    ///
    /// Retrieval is priced separately by [`Self::retrieval_nanodollars`]: it
    /// applies to the bytes a restore-class read pulls out of storage, which is
    /// not the same quantity as the bytes a pass transfers out.
    pub fn modeled_nanodollars(
        &self,
        put_class_requests: u64,
        get_class_requests: u64,
        transfer_bytes: u64,
    ) -> u64 {
        let puts = put_class_requests.saturating_mul(self.put_class_nanodollars);
        let gets = get_class_requests.saturating_mul(self.get_class_nanodollars);
        puts.saturating_add(gets)
            .saturating_add(per_gib_nanodollars(
                transfer_bytes,
                self.transfer_nanodollars_per_gib,
            ))
    }

    /// Nanodollars for `retrieval_bytes` pulled from a retrieval-billed storage
    /// class, prorated from [`Self::retrieval_nanodollars_per_gib`] the same
    /// exact way [`Self::modeled_nanodollars`] prorates transfer.
    pub fn retrieval_nanodollars(&self, retrieval_bytes: u64) -> u64 {
        per_gib_nanodollars(retrieval_bytes, self.retrieval_nanodollars_per_gib)
    }
}

/// `bytes * nanodollars_per_gib / 2^30`, truncated toward zero, saturating at
/// `u64::MAX`. The product is formed in `u128` so a large byte count times a
/// large price cannot lose the low bits before the division.
fn per_gib_nanodollars(bytes: u64, nanodollars_per_gib: u64) -> u64 {
    let total = u128::from(bytes) * u128::from(nanodollars_per_gib) / BYTES_PER_GIB;
    u64::try_from(total).unwrap_or(u64::MAX)
}

/// Why a store cost profile could not be loaded or rendered. A malformed
/// document is always one of these, never a panic.
#[derive(Debug, thiserror::Error)]
pub enum CostProfileError {
    /// The document is not valid TOML, is missing a required price, or carries
    /// a field the profile does not define (unknown fields are refused, so a
    /// misspelled key fails loudly instead of leaving a default in force).
    #[error("invalid store cost profile TOML: {0}")]
    Toml(#[from] toml::de::Error),
    /// The profile carries no name, so nothing could identify it in a
    /// provenance stamp.
    #[error("store cost profile name must not be empty")]
    EmptyName,
    /// The profile could not be rendered back to TOML.
    #[error("could not render store cost profile as TOML: {0}")]
    Render(#[from] toml::ser::Error),
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn reference_profile_prices_are_pinned_exactly() {
        // These are contract figures, not measurements: $5.00 and $0.40 per
        // million requests, in nanodollars ($1 = 1e9). A change here changes
        // every modeled-cost figure the ledger publishes, so it is pinned to
        // the arithmetic rather than to a repeated literal.
        let p = StoreCostProfile::reference();
        assert_eq!(p.name, "s3-intra-region-2026");
        assert_eq!(
            p.put_class_nanodollars * 1_000_000,
            5_000_000_000,
            "PUT class is exactly $5.00 per million requests"
        );
        assert_eq!(
            p.get_class_nanodollars * 1_000_000,
            400_000_000,
            "GET class is exactly $0.40 per million requests"
        );
        assert_eq!(
            p.transfer_nanodollars_per_gib, 0,
            "intra-region transfer is free"
        );
        assert_eq!(
            p.retrieval_nanodollars_per_gib, 0,
            "standard-class retrieval is free"
        );
    }

    #[test]
    fn reference_profile_put_get_ratio_is_exactly_twelve_and_a_half() {
        // The 12.5:1 ratio is the fact ADR-0996 reasons from: one PUT costs
        // 12.5 GETs, so trading a request for bytes inverts the read path's
        // economics. Asserted in exact integers (12.5 has no exact u64 form,
        // 25/2 does), never as a float division.
        let p = StoreCostProfile::reference();
        assert_eq!(
            p.put_class_nanodollars * 2,
            p.get_class_nanodollars * 25,
            "PUT/GET is exactly 25/2 = 12.5; a drift in either price breaks this \
             before it silently reprices every modeled figure"
        );
    }

    #[test]
    fn op_classes_put_list_and_get_head_as_s3_bills_them() {
        // The two non-obvious mappings, which are the whole reason a class
        // function exists instead of a price per op.
        assert_eq!(CostedStoreOp::List.class(), StoreOpClass::Put);
        assert_eq!(CostedStoreOp::ListDelimited.class(), StoreOpClass::Put);
        assert_eq!(CostedStoreOp::Head.class(), StoreOpClass::Get);
        assert_eq!(CostedStoreOp::Put.class(), StoreOpClass::Put);
        assert_eq!(CostedStoreOp::Get.class(), StoreOpClass::Get);
        assert_eq!(CostedStoreOp::Delete.class(), StoreOpClass::Delete);
        // Priced class, zero PRICE on the reference profile: a provider that
        // bills deletes overrides the field, never a hardcoded free tier.
        assert_eq!(
            StoreCostProfile::reference().op_nanodollars(CostedStoreOp::Delete),
            0
        );

        let p = StoreCostProfile::reference();
        assert_eq!(
            p.op_nanodollars(CostedStoreOp::List),
            5_000,
            "a LIST costs a PUT, not a GET"
        );
        assert_eq!(p.op_nanodollars(CostedStoreOp::Head), 400);
        assert_eq!(p.op_nanodollars(CostedStoreOp::Delete), 0);

        // Every variant prices at exactly its class rate, so a new op added to
        // the mirror of `StoreOp` cannot end up priced by some other path.
        for op in CostedStoreOp::ALL {
            assert_eq!(
                p.op_nanodollars(op),
                p.class_nanodollars(op.class()),
                "{op:?} must price at its class rate"
            );
        }
    }

    #[test]
    fn accounted_op_prices_through_the_same_class_mapping() {
        // The query path counts AccountedOp; pricing must not need a second
        // mapping table that could drift from CostedStoreOp::class.
        let p = StoreCostProfile::reference();
        assert_eq!(p.op_nanodollars(AccountedOp::Get), 400);
        assert_eq!(p.op_nanodollars(AccountedOp::Head), 400);
        assert_eq!(
            p.op_nanodollars(AccountedOp::List),
            5_000,
            "a query's LIST is billed PUT-class like any other"
        );
        for op in AccountedOp::ALL {
            assert_eq!(
                p.op_nanodollars(op),
                p.class_nanodollars(CostedStoreOp::from(op).class()),
                "every AccountedOp prices through CostedStoreOp::class"
            );
        }
    }

    #[test]
    fn modeled_cost_of_the_cold_pass_fixture_is_exact() {
        // 149_167 GET-class requests at the reference profile:
        //   149_167 * 400 = 59_666_800 nanodollars = $0.0596668.
        // Cross-checked against the price definition rather than the literal:
        // 149_167 requests at $0.40 per million is 149_167 * 4 / 10 microdollars
        // = 59_666.8 microdollars = 59_666_800 nanodollars.
        let p = StoreCostProfile::reference();
        let modeled = p.modeled_nanodollars(0, 149_167, 0);
        assert_eq!(modeled, 59_666_800);
        assert_eq!(
            modeled,
            149_167 * p.get_class_nanodollars,
            "the fixture figure is requests times the GET-class price, nothing else"
        );
        assert_eq!(
            p.modeled_nanodollars(0, 1_000_000, 0),
            400_000_000,
            "a million GETs is $0.40 exactly"
        );
        assert_eq!(
            p.modeled_nanodollars(1_000_000, 0, 0),
            5_000_000_000,
            "a million PUTs is $5.00 exactly"
        );
    }

    #[test]
    fn modeled_cost_sums_both_classes_and_transfer() {
        let p = StoreCostProfile {
            name: "egress-billed".to_string(),
            put_class_nanodollars: 5_000,
            get_class_nanodollars: 400,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 90_000_000, // $0.09/GiB
            retrieval_nanodollars_per_gib: 10_000_000,
        };
        // 3 PUTs + 7 GETs + exactly 2 GiB transferred.
        let two_gib = 2 * 1024 * 1024 * 1024;
        assert_eq!(
            p.modeled_nanodollars(3, 7, two_gib),
            15_000 + 2_800 + 180_000_000
        );
        assert_eq!(p.retrieval_nanodollars(two_gib), 20_000_000);
    }

    #[test]
    fn transfer_proration_truncates_and_never_uses_a_float() {
        let p = StoreCostProfile {
            name: "prorate".to_string(),
            put_class_nanodollars: 0,
            get_class_nanodollars: 0,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 1_000_000_000, // $1/GiB
            retrieval_nanodollars_per_gib: 0,
        };
        // Half a GiB is half a dollar, exactly.
        assert_eq!(p.modeled_nanodollars(0, 0, 512 * 1024 * 1024), 500_000_000);
        // One byte is 1e9/2^30 nanodollars = 0.93..., truncated to 0. A bill is
        // never rounded up by the model.
        assert_eq!(p.modeled_nanodollars(0, 0, 1), 0);
        // Two bytes is 1.86..., still truncated to 1.
        assert_eq!(p.modeled_nanodollars(0, 0, 2), 1);
    }

    #[test]
    fn modeled_cost_saturates_instead_of_wrapping() {
        let p = StoreCostProfile {
            name: "absurd".to_string(),
            put_class_nanodollars: u64::MAX,
            get_class_nanodollars: u64::MAX,
            delete_class_nanodollars: u64::MAX,
            transfer_nanodollars_per_gib: u64::MAX,
            retrieval_nanodollars_per_gib: u64::MAX,
        };
        assert_eq!(
            p.modeled_nanodollars(u64::MAX, u64::MAX, u64::MAX),
            u64::MAX,
            "a saturated modeled cost reads as at-least, never wrapping to cheap"
        );
        assert_eq!(p.retrieval_nanodollars(u64::MAX), u64::MAX);
    }

    #[test]
    fn toml_round_trip_preserves_every_field() {
        let original = StoreCostProfile {
            name: "s3-cross-region-test".to_string(),
            put_class_nanodollars: 5_500,
            get_class_nanodollars: 440,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 20_000_000,
            retrieval_nanodollars_per_gib: 1_234,
        };
        let rendered = original.to_toml_string().expect("render");
        let parsed = StoreCostProfile::from_toml_str(&rendered).expect("parse");
        assert_eq!(parsed, original);
    }

    #[test]
    fn toml_loads_a_hand_written_document() {
        let doc = r#"
name = "s3-intra-region-2026"
put_class_nanodollars = 5000
get_class_nanodollars = 400
transfer_nanodollars_per_gib = 0
retrieval_nanodollars_per_gib = 0
"#;
        let parsed = StoreCostProfile::from_toml_str(doc).expect("parse");
        assert_eq!(parsed, StoreCostProfile::reference());
    }

    #[test]
    fn unknown_field_is_a_typed_error_not_a_silent_default() {
        // A misspelled price key must fail loudly: accepted-and-ignored would
        // leave the reference price in force while the report stamps the
        // operator's file, which is the provenance failure this type prevents.
        let doc = r#"
name = "typo"
put_class_nanodollars = 5000
get_class_nanodollars = 400
transfer_nanodollars_per_gib = 0
retrieval_nanodollars_per_gib = 0
get_class_nanodollar = 1
"#;
        let err = StoreCostProfile::from_toml_str(doc).expect_err("unknown field must be refused");
        assert!(
            matches!(err, CostProfileError::Toml(_)),
            "unknown field is a typed TOML error, got {err:?}"
        );
        assert!(
            err.to_string().contains("get_class_nanodollar"),
            "the error names the offending key: {err}"
        );
    }

    #[test]
    fn missing_price_and_wrong_type_are_typed_errors() {
        let missing = StoreCostProfile::from_toml_str("name = \"x\"\n")
            .expect_err("a missing price must be refused");
        assert!(matches!(missing, CostProfileError::Toml(_)));

        let float_price = StoreCostProfile::from_toml_str(
            "name = \"x\"\nput_class_nanodollars = 5000.0\nget_class_nanodollars = 400\n\
             transfer_nanodollars_per_gib = 0\nretrieval_nanodollars_per_gib = 0\n",
        )
        .expect_err("a float price must be refused: nanodollars are integers");
        assert!(matches!(float_price, CostProfileError::Toml(_)));
    }

    #[test]
    fn empty_name_is_refused_so_a_stamp_can_never_be_anonymous() {
        let doc = r#"
name = "   "
put_class_nanodollars = 5000
get_class_nanodollars = 400
transfer_nanodollars_per_gib = 0
retrieval_nanodollars_per_gib = 0
"#;
        let err = StoreCostProfile::from_toml_str(doc).expect_err("blank name must be refused");
        assert!(matches!(err, CostProfileError::EmptyName), "got {err:?}");
    }
}
