//! ravel-invariant: the assertion and mutant macros that carry the TLA+
//! invariants under `formal/tla` into the binaries as Antithesis properties
//! (ADR-4326 decisions 1 and 3).
//!
//! - [`inv_always!`], [`inv_always_or_unreachable!`] and [`inv_sometimes!`]
//!   take a condition, a message literal and a details expression;
//!   [`inv_always_if_known!`] takes an `Option<bool>` in place of the
//!   condition and evaluates nothing for `None`;
//!   [`inv_reachable!`] and [`inv_unreachable!`] take the message and the
//!   details. The message is the property's name in Antithesis and in the
//!   traceability column, so it is a literal.
//! - [`inv_mutant!`]`("<area>/<negative cfg stem>")` is the test-only switch a
//!   mutant control flips: `if inv_mutant!("commit/commit-before-data") { .. }`.
//!
//! Every feature is off by default and none is in a published build
//! (`scripts/guards/check-release-profile.sh` refuses `antithesis`,
//! `antithesis-mutants` and `capture` there):
//!
//! - `antithesis`: each assertion calls the Antithesis SDK at its site, and
//!   [`init`] registers the assertion catalog.
//! - `capture`: each assertion records its evaluation in this process
//!   (`capture` module), for crate tests. No SDK.
//! - `antithesis-mutants`: [`inv_mutant!`] is true for the one mutant
//!   [`init`] selected from `RAVEL_ANTITHESIS_MUTANT` (or a test scoped with
//!   `mutant::scoped`), and its name must be in `mutant::MUTANTS`.
//!
//! With neither `antithesis` nor `capture`, each assertion macro expands to
//! nothing; without `antithesis-mutants`, [`inv_mutant!`] expands to `false`.
//! Neither evaluates its arguments then, so an argument must not carry a side
//! effect the code relies on.

use std::fmt;

#[cfg(feature = "antithesis")]
#[doc(hidden)]
pub use antithesis_sdk as __sdk;

/// The environment variable that selects the one mutant a mutant image runs.
pub const MUTANT_ENV: &str = "RAVEL_ANTITHESIS_MUTANT";

/// Why [`init`] refused to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitError {
    /// `RAVEL_ANTITHESIS_MUTANT` (or a test's scoped selection) names no
    /// registered mutant.
    UnknownMutant { name: String },
    /// `RAVEL_ANTITHESIS_MUTANT` is not valid UTF-8.
    MutantNotUnicode,
}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InitError::UnknownMutant { name } => write!(
                f,
                "{MUTANT_ENV}={name:?} names no registered mutant; refusing to start \
                 (the registered names are in ravel_invariant::mutant::MUTANTS)"
            ),
            InitError::MutantNotUnicode => {
                write!(f, "{MUTANT_ENV} is not valid UTF-8; refusing to start")
            }
        }
    }
}

impl std::error::Error for InitError {}

/// Process start for a binary: call once, first thing in `main`, and refuse
/// to start on an error.
///
/// With `antithesis` it registers the assertion catalog with Antithesis, so
/// an assertion that is never reached still reports. With
/// `antithesis-mutants` it reads `RAVEL_ANTITHESIS_MUTANT` once and selects
/// that mutant for the life of the process; unset or empty selects none, and
/// a name `mutant::MUTANTS` does not hold is [`InitError::UnknownMutant`].
/// With neither feature it does nothing and reads nothing.
pub fn init() -> Result<(), InitError> {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();
    #[cfg(feature = "antithesis-mutants")]
    mutant::init_from_env()?;
    Ok(())
}

/// The SDK call of one assertion, or nothing without `antithesis`.
#[cfg(feature = "antithesis")]
#[doc(hidden)]
#[macro_export]
macro_rules! __inv_sdk {
    (always, $condition:ident, $message:literal, $details:ident) => {
        $crate::__sdk::assert_always!($condition, $message, $details)
    };
    (always_or_unreachable, $condition:ident, $message:literal, $details:ident) => {
        $crate::__sdk::assert_always_or_unreachable!($condition, $message, $details)
    };
    (sometimes, $condition:ident, $message:literal, $details:ident) => {
        $crate::__sdk::assert_sometimes!($condition, $message, $details)
    };
    (reachable, $condition:ident, $message:literal, $details:ident) => {{
        let _ = $condition;
        $crate::__sdk::assert_reachable!($message, $details)
    }};
    (unreachable, $condition:ident, $message:literal, $details:ident) => {{
        let _ = $condition;
        $crate::__sdk::assert_unreachable!($message, $details)
    }};
}
#[cfg(not(feature = "antithesis"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __inv_sdk {
    ($($tokens:tt)*) => {};
}

/// The capture record of one assertion, or nothing without `capture`.
#[cfg(feature = "capture")]
#[doc(hidden)]
#[macro_export]
macro_rules! __inv_capture {
    ($kind:ident, $condition:ident, $message:literal, $details:ident) => {
        $crate::capture::record($crate::capture::Evaluation {
            kind: $crate::capture::Kind::$kind,
            message: $message,
            condition: $condition,
            details: ::std::format!("{:?}", $details),
            file: ::std::file!(),
            line: ::std::line!(),
        })
    };
}
#[cfg(not(feature = "capture"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __inv_capture {
    ($($tokens:tt)*) => {};
}

/// One assertion with both carriers; each expands to nothing when its
/// feature is off. The arguments are evaluated once, here.
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __inv_assert {
    ($sdk:ident, $kind:ident, $condition:expr, $message:literal, $details:expr) => {{
        let condition: bool = $condition;
        let details = &$details;
        $crate::__inv_sdk!($sdk, condition, $message, details);
        $crate::__inv_capture!($kind, condition, $message, details);
    }};
}

/// `condition` holds every time this site runs, and the site runs at least
/// once in a run.
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_always {
    ($condition:expr, $message:literal, $details:expr $(,)?) => {
        $crate::__inv_assert!(always, Always, $condition, $message, $details)
    };
}
/// `condition` holds every time this site runs, and the site runs at least
/// once in a run.
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_always {
    ($condition:expr, $message:literal, $details:expr $(,)?) => {};
}

/// [`inv_always!`] over a verdict that may be unknown: `verdict` is an
/// `Option<bool>`, evaluated as `inv_always!` evaluates its condition when it
/// is `Some`, and not evaluated at all when it is `None` (the site could not
/// tell, which is neither a pass nor a failure).
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_always_if_known {
    ($verdict:expr, $message:literal, $details:expr $(,)?) => {{
        let verdict: ::std::option::Option<bool> = $verdict;
        if let ::std::option::Option::Some(condition) = verdict {
            $crate::__inv_assert!(always, Always, condition, $message, $details)
        }
    }};
}
/// [`inv_always!`] over a verdict that may be unknown: `verdict` is an
/// `Option<bool>`, evaluated as `inv_always!` evaluates its condition when it
/// is `Some`, and not evaluated at all when it is `None` (the site could not
/// tell, which is neither a pass nor a failure).
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_always_if_known {
    ($verdict:expr, $message:literal, $details:expr $(,)?) => {};
}

/// `condition` holds every time this site runs; a run that never reaches it
/// passes. Only on a guard only a fault reaches (ADR-4326 decision 1).
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_always_or_unreachable {
    ($condition:expr, $message:literal, $details:expr $(,)?) => {
        $crate::__inv_assert!(
            always_or_unreachable,
            AlwaysOrUnreachable,
            $condition,
            $message,
            $details
        )
    };
}
/// `condition` holds every time this site runs; a run that never reaches it
/// passes. Only on a guard only a fault reaches (ADR-4326 decision 1).
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_always_or_unreachable {
    ($condition:expr, $message:literal, $details:expr $(,)?) => {};
}

/// `condition` holds at least once in a run.
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_sometimes {
    ($condition:expr, $message:literal, $details:expr $(,)?) => {
        $crate::__inv_assert!(sometimes, Sometimes, $condition, $message, $details)
    };
}
/// `condition` holds at least once in a run.
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_sometimes {
    ($condition:expr, $message:literal, $details:expr $(,)?) => {};
}

/// This site runs at least once in a run.
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_reachable {
    ($message:literal, $details:expr $(,)?) => {
        $crate::__inv_assert!(reachable, Reachable, true, $message, $details)
    };
}
/// This site runs at least once in a run.
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_reachable {
    ($message:literal, $details:expr $(,)?) => {};
}

/// This site never runs.
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_unreachable {
    ($message:literal, $details:expr $(,)?) => {
        $crate::__inv_assert!(unreachable, Unreachable, false, $message, $details)
    };
}
/// This site never runs.
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_unreachable {
    ($message:literal, $details:expr $(,)?) => {};
}

/// True when the named mutant is selected. The name must be in
/// `mutant::MUTANTS`, which a build with `antithesis-mutants` checks at
/// compile time. Without that feature this is the constant `false`.
#[cfg(feature = "antithesis-mutants")]
#[macro_export]
macro_rules! inv_mutant {
    ($name:literal) => {{
        const _: () = ::std::assert!(
            $crate::mutant::is_registered($name),
            ::std::concat!(
                "inv_mutant!: ",
                $name,
                " is not in ravel_invariant::mutant::MUTANTS"
            ),
        );
        $crate::mutant::is_selected($name)
    }};
}
/// True when the named mutant is selected. The name must be in
/// `mutant::MUTANTS`, which a build with `antithesis-mutants` checks at
/// compile time. Without that feature this is the constant `false`.
#[cfg(not(feature = "antithesis-mutants"))]
#[macro_export]
macro_rules! inv_mutant {
    ($name:literal) => {
        false
    };
}

/// In-process record of every assertion evaluation, for crate tests.
#[cfg(feature = "capture")]
pub mod capture {
    use std::sync::{Mutex, MutexGuard, PoisonError};

    /// Which macro made the evaluation.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Kind {
        Always,
        AlwaysOrUnreachable,
        Sometimes,
        Reachable,
        Unreachable,
    }

    /// One evaluation of one assertion site.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Evaluation {
        pub kind: Kind,
        pub message: &'static str,
        /// The condition as evaluated: always `true` for a reachable and
        /// `false` for an unreachable, as the SDK records them.
        pub condition: bool,
        /// The details argument, `Debug`-formatted.
        pub details: String,
        pub file: &'static str,
        pub line: u32,
    }

    impl Evaluation {
        /// True when this evaluation alone fails its property: a false
        /// always, or a reached unreachable. A false sometimes is not one.
        pub fn is_violation(&self) -> bool {
            match self.kind {
                Kind::Always | Kind::AlwaysOrUnreachable => !self.condition,
                Kind::Unreachable => true,
                Kind::Sometimes | Kind::Reachable => false,
            }
        }
    }

    static EVALUATIONS: Mutex<Vec<Evaluation>> = Mutex::new(Vec::new());

    // A test that panicked while holding the lock leaves the record intact;
    // the next reader still sees every evaluation made before the panic.
    fn lock() -> MutexGuard<'static, Vec<Evaluation>> {
        EVALUATIONS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[doc(hidden)]
    pub fn record(evaluation: Evaluation) {
        lock().push(evaluation);
    }

    /// Every evaluation this process made so far, in order.
    pub fn evaluations() -> Vec<Evaluation> {
        lock().clone()
    }

    /// The evaluations of one property, in order. Tests in one binary share
    /// the record, so a test reads its own sites by message.
    pub fn evaluations_of(message: &str) -> Vec<Evaluation> {
        lock()
            .iter()
            .filter(|e| e.message == message)
            .cloned()
            .collect()
    }
}

/// The mutant switch: the registered names, the process selection and the
/// per-test scope.
#[cfg(feature = "antithesis-mutants")]
pub mod mutant {
    use std::cell::Cell;
    use std::ffi::OsStr;
    use std::marker::PhantomData;
    use std::sync::OnceLock;

    use crate::{InitError, MUTANT_ENV};

    /// Every mutant `inv_mutant!` may name, `<area>/<negative cfg stem>`
    /// (ADR-4326 decision 3). The common area's mutants live in the
    /// store-contract driver's broken store, not here.
    pub const MUTANTS: &[&str] = &[
        "commit/ack-before-commit",
        "commit/commit-before-data",
        "commit/mismatched-identity-idempotent",
        "commit/marker-before-all-shards",
        "catalog/lost-cas-proceeds",
        "catalog/compaction-swaps-record",
        "catalog/compaction-loser-overwrites",
        "catalog/sweep-superseded-no-head-gate",
        "catalog/corrupt-head-ignores-delete-gate",
        "lifecycle/refresh-failure-is-no-hold",
        "lifecycle/delete-before-horizon",
        "lifecycle/rewrite-keeps-erased-records",
        "lifecycle/complete-ignores-served-set",
        "maintenance/mo-diverge-overwrites-record",
        "maintenance/mo-missing-part-reports-converged",
        "resharding/no-writer-fence",
        "resharding/token-validated-against-count",
    ];

    const fn str_eq(a: &str, b: &str) -> bool {
        let (a, b) = (a.as_bytes(), b.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        let mut i = 0;
        while i < a.len() {
            if a[i] != b[i] {
                return false;
            }
            i += 1;
        }
        true
    }

    /// Whether `name` is in [`MUTANTS`]; `inv_mutant!` asserts it at compile
    /// time.
    pub const fn is_registered(name: &str) -> bool {
        let mut i = 0;
        while i < MUTANTS.len() {
            if str_eq(MUTANTS[i], name) {
                return true;
            }
            i += 1;
        }
        false
    }

    fn registered(name: &str) -> Result<&'static str, InitError> {
        MUTANTS
            .iter()
            .copied()
            .find(|m| *m == name)
            .ok_or_else(|| InitError::UnknownMutant {
                name: name.to_owned(),
            })
    }

    /// The mutant a value of `RAVEL_ANTITHESIS_MUTANT` selects: none when
    /// unset or empty, the registered name otherwise, and an error for any
    /// other value.
    pub fn parse_selection(value: Option<&OsStr>) -> Result<Option<&'static str>, InitError> {
        let Some(value) = value else {
            return Ok(None);
        };
        let value = value.to_str().ok_or(InitError::MutantNotUnicode)?;
        if value.is_empty() {
            return Ok(None);
        }
        registered(value).map(Some)
    }

    static PROCESS: OnceLock<Option<&'static str>> = OnceLock::new();

    pub(crate) fn init_from_env() -> Result<(), InitError> {
        let selection = parse_selection(std::env::var_os(MUTANT_ENV).as_deref())?;
        // Read once: a second call keeps the first selection.
        let _ = PROCESS.set(selection);
        Ok(())
    }

    thread_local! {
        static SCOPED: Cell<Option<Option<&'static str>>> = const { Cell::new(None) };
    }

    /// Whether `name` is the selected mutant on this thread: the test scope
    /// when one is open here, the process selection otherwise. Before
    /// [`crate::init`] the process selects none.
    pub fn is_selected(name: &str) -> bool {
        let selected = SCOPED
            .with(Cell::get)
            .unwrap_or_else(|| PROCESS.get().copied().flatten());
        selected == Some(name)
    }

    /// Selects `name` on the calling thread until the guard drops, for a
    /// test. It shadows the process selection there and nowhere else, so
    /// tests in one binary do not see each other's mutant. Code the test
    /// hands to another thread (a multi-thread runtime's workers) does not
    /// see it either: drive the mutant site on the test's own thread.
    pub fn scoped(name: &str) -> Result<ScopedMutant, InitError> {
        let name = registered(name)?;
        let previous = SCOPED.with(|s| s.replace(Some(Some(name))));
        Ok(ScopedMutant {
            previous,
            _not_send: PhantomData,
        })
    }

    /// Restores the calling thread's previous selection on drop.
    ///
    /// It is not `Send`: dropped on another thread it would restore that
    /// thread's slot and leave the selection open on this one.
    ///
    /// ```compile_fail,E0277
    /// fn assert_send<T: Send>() {}
    /// assert_send::<ravel_invariant::mutant::ScopedMutant>();
    /// ```
    #[must_use = "the mutant is deselected when this guard drops"]
    pub struct ScopedMutant {
        previous: Option<Option<&'static str>>,
        _not_send: PhantomData<*const ()>,
    }

    impl Drop for ScopedMutant {
        fn drop(&mut self) {
            SCOPED.with(|s| s.set(self.previous));
        }
    }

    // Fails to compile if ScopedMutant becomes Send: the call below is then
    // ambiguous between the two impls.
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    struct IsSend;
    impl<T: ?Sized + Send> AmbiguousIfSend<IsSend> for T {}
    const _: fn() = || <ScopedMutant as AmbiguousIfSend<_>>::check();
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    #[cfg(not(any(
        feature = "antithesis",
        feature = "capture",
        feature = "antithesis-mutants"
    )))]
    #[test]
    fn macros_expand_to_nothing_without_the_feature() {
        let evaluated = std::cell::Cell::new(0u32);
        inv_always!(
            {
                evaluated.set(evaluated.get() + 1);
                false
            },
            "ravel-invariant test: an always is not evaluated",
            evaluated.set(evaluated.get() + 1)
        );
        inv_always_or_unreachable!(
            {
                evaluated.set(evaluated.get() + 1);
                false
            },
            "ravel-invariant test: an always-or-unreachable is not evaluated",
            evaluated.set(evaluated.get() + 1)
        );
        inv_sometimes!(
            {
                evaluated.set(evaluated.get() + 1);
                true
            },
            "ravel-invariant test: a sometimes is not evaluated",
            evaluated.set(evaluated.get() + 1)
        );
        inv_always_if_known!(
            {
                evaluated.set(evaluated.get() + 1);
                Some(false)
            },
            "ravel-invariant test: an always-if-known is not evaluated",
            evaluated.set(evaluated.get() + 1)
        );
        inv_reachable!(
            "ravel-invariant test: a reachable is not evaluated",
            evaluated.set(evaluated.get() + 1)
        );
        inv_unreachable!(
            "ravel-invariant test: an unreachable is not evaluated",
            evaluated.set(evaluated.get() + 1)
        );
        assert_eq!(
            evaluated.get(),
            0,
            "an assertion macro evaluated an argument"
        );

        let selected = inv_mutant!("commit/commit-before-data");
        assert!(!selected, "inv_mutant! is false without the feature");
    }

    // Invokes each assertion macro once under `antithesis` so a build with that
    // feature (and without `capture`) expands and type-checks every
    // `__inv_sdk!` arm; without this, no gate compiles the always,
    // always_or_unreachable or sometimes arms. The SDK no-ops outside an
    // Antithesis run, so this need not assert.
    #[cfg(feature = "antithesis")]
    #[test]
    fn antithesis_sdk_arms_type_check() {
        let one = 1;
        inv_always!(
            one == 1,
            "ravel-invariant test: antithesis always arm",
            serde_json::json!({ "one": one })
        );
        inv_always_or_unreachable!(
            one == 1,
            "ravel-invariant test: antithesis always-or-unreachable arm",
            serde_json::json!({ "one": one })
        );
        inv_always_if_known!(
            Some(one == 1),
            "ravel-invariant test: antithesis always-if-known arm",
            serde_json::json!({ "one": one })
        );
        inv_sometimes!(
            one == 1,
            "ravel-invariant test: antithesis sometimes arm",
            serde_json::json!({ "one": one })
        );
        inv_reachable!(
            "ravel-invariant test: antithesis reachable arm",
            serde_json::json!({ "one": one })
        );
        inv_unreachable!(
            "ravel-invariant test: antithesis unreachable arm",
            serde_json::json!({ "one": one })
        );
        let _selected = inv_mutant!("commit/commit-before-data");
    }

    #[cfg(feature = "capture")]
    #[test]
    fn capture_records_a_false_always() {
        use crate::capture::{Kind, evaluations_of};

        const HELD: &str = "ravel-invariant test: a true always is recorded as held";
        const FALSE: &str = "ravel-invariant test: a false always is recorded";
        let sum = 1 + 1;
        inv_always!(
            sum == 2,
            "ravel-invariant test: a true always is recorded as held",
            serde_json::json!({ "sum": sum })
        );
        inv_always!(
            sum == 3,
            "ravel-invariant test: a false always is recorded",
            serde_json::json!({ "sum": sum })
        );

        let held = evaluations_of(HELD);
        assert_eq!(held.len(), 1, "{held:?}");
        assert_eq!(held[0].kind, Kind::Always);
        assert!(held[0].condition);
        assert!(!held[0].is_violation());

        let broken = evaluations_of(FALSE);
        assert_eq!(broken.len(), 1, "{broken:?}");
        assert_eq!(broken[0].kind, Kind::Always);
        assert!(!broken[0].condition);
        assert!(broken[0].is_violation());
        assert!(
            broken[0].details.contains("\"sum\""),
            "{}",
            broken[0].details
        );
        assert!(broken[0].details.contains('2'), "{}", broken[0].details);
        assert_eq!(broken[0].file, file!());
    }

    // `None` records nothing; `Some` records exactly what `inv_always!` does
    // with the same condition, kind included.
    #[cfg(feature = "capture")]
    #[test]
    fn capture_records_an_always_if_known_only_when_known() {
        use crate::capture::{Kind, evaluations_of};

        const UNKNOWN: &str = "ravel-invariant test: an unknown always-if-known";
        const HELD: &str = "ravel-invariant test: a held always-if-known";
        const FALSE: &str = "ravel-invariant test: a false always-if-known";
        let details_evaluated = std::cell::Cell::new(0u32);
        inv_always_if_known!(
            None,
            "ravel-invariant test: an unknown always-if-known",
            details_evaluated.set(details_evaluated.get() + 1)
        );
        inv_always_if_known!(
            Some(true),
            "ravel-invariant test: a held always-if-known",
            serde_json::json!({ "known": true })
        );
        inv_always_if_known!(
            Some(false),
            "ravel-invariant test: a false always-if-known",
            serde_json::json!({ "known": false })
        );

        assert_eq!(evaluations_of(UNKNOWN), Vec::new());
        assert_eq!(details_evaluated.get(), 0, "None evaluates no details");
        let held = evaluations_of(HELD);
        assert_eq!(held.len(), 1, "{held:?}");
        assert_eq!(held[0].kind, Kind::Always);
        assert!(held[0].condition && !held[0].is_violation());
        assert_eq!(held[0].file, file!());
        let broken = evaluations_of(FALSE);
        assert_eq!(broken.len(), 1, "{broken:?}");
        assert_eq!(broken[0].kind, Kind::Always);
        assert!(!broken[0].condition && broken[0].is_violation());
        assert!(
            broken[0].details.contains("\"known\""),
            "{}",
            broken[0].details
        );
    }

    #[cfg(feature = "capture")]
    #[test]
    fn capture_records_reachable_and_unreachable_as_the_sdk_does() {
        use crate::capture::{Kind, evaluations_of};

        inv_reachable!(
            "ravel-invariant test: a reached reachable",
            serde_json::json!({})
        );
        inv_unreachable!(
            "ravel-invariant test: a reached unreachable",
            serde_json::json!({})
        );
        let reached = evaluations_of("ravel-invariant test: a reached reachable");
        assert_eq!(reached.len(), 1);
        assert_eq!(reached[0].kind, Kind::Reachable);
        assert!(reached[0].condition && !reached[0].is_violation());
        let unreachable = evaluations_of("ravel-invariant test: a reached unreachable");
        assert_eq!(unreachable.len(), 1);
        assert_eq!(unreachable[0].kind, Kind::Unreachable);
        assert!(!unreachable[0].condition && unreachable[0].is_violation());
    }

    #[cfg(feature = "antithesis-mutants")]
    #[test]
    fn unknown_mutant_name_is_refused() {
        use std::ffi::OsStr;

        use crate::InitError;
        use crate::mutant::{parse_selection, scoped};

        let unknown = InitError::UnknownMutant {
            name: "commit/no-such-mutant".to_owned(),
        };
        assert_eq!(
            parse_selection(Some(OsStr::new("commit/no-such-mutant"))),
            Err(unknown.clone())
        );
        assert!(
            unknown.to_string().contains("refusing to start"),
            "{unknown}"
        );
        assert_eq!(
            scoped("commit/no-such-mutant").err(),
            Some(unknown),
            "a test scope refuses the same names"
        );

        assert_eq!(
            parse_selection(Some(OsStr::new("commit/commit-before-data"))),
            Ok(Some("commit/commit-before-data"))
        );
        assert_eq!(parse_selection(Some(OsStr::new(""))), Ok(None));
        assert_eq!(parse_selection(None), Ok(None));
    }

    #[cfg(all(feature = "antithesis-mutants", unix))]
    #[test]
    fn non_unicode_mutant_value_is_refused() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        assert_eq!(
            crate::mutant::parse_selection(Some(OsStr::from_bytes(b"commit/\xff"))),
            Err(crate::InitError::MutantNotUnicode)
        );
    }

    // Two tests in one binary, run as two threads that each select a
    // different mutant. Both hold their selection before either reads and
    // until both have read, so a process-wide selection would show one of
    // them the other's mutant.
    #[cfg(feature = "antithesis-mutants")]
    #[test]
    fn mutant_selection_is_scoped_to_one_test() {
        use std::sync::{Arc, Barrier};

        use crate::mutant::scoped;

        let both_selected = Arc::new(Barrier::new(2));
        let both_read = Arc::new(Barrier::new(2));
        let first = {
            let both_selected = Arc::clone(&both_selected);
            let both_read = Arc::clone(&both_read);
            std::thread::spawn(move || {
                let guard = scoped("commit/commit-before-data").expect("a registered mutant");
                both_selected.wait();
                let seen = (
                    inv_mutant!("commit/commit-before-data"),
                    inv_mutant!("catalog/lost-cas-proceeds"),
                );
                both_read.wait();
                drop(guard);
                let after = inv_mutant!("commit/commit-before-data");
                (seen, after)
            })
        };
        let second = {
            let both_selected = Arc::clone(&both_selected);
            let both_read = Arc::clone(&both_read);
            std::thread::spawn(move || {
                let guard = scoped("catalog/lost-cas-proceeds").expect("a registered mutant");
                both_selected.wait();
                let seen = (
                    inv_mutant!("commit/commit-before-data"),
                    inv_mutant!("catalog/lost-cas-proceeds"),
                );
                both_read.wait();
                drop(guard);
                let after = inv_mutant!("catalog/lost-cas-proceeds");
                (seen, after)
            })
        };
        let (first, second) = (
            first.join().expect("first test thread"),
            second.join().expect("second test thread"),
        );
        assert_eq!(first, ((true, false), false), "first test's view");
        assert_eq!(second, ((false, true), false), "second test's view");
        assert!(
            !inv_mutant!("commit/commit-before-data") && !inv_mutant!("catalog/lost-cas-proceeds"),
            "a thread that opened no scope sees no mutant"
        );
    }
}
