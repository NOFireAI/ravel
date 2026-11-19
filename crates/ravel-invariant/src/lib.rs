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
//! - [`inv_fault!`]`("<area>/<name>")` is the switch a fault run flips: it
//!   fires at the rate `RAVEL_ANTITHESIS_FAULTS` gives that name, and its
//!   site then takes a failure path the SUT already answers. A mutant run
//!   expects a property to go false; a fault run expects every property to
//!   hold.
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
//! With `antithesis` or `capture`, [`inv_fault!`] is compiled in: [`init`]
//! reads `RAVEL_ANTITHESIS_FAULTS`, a test sets a rate with `fault::scoped`,
//! and the name must be in `fault::FAULTS`.
//!
//! With neither `antithesis` nor `capture`, each assertion macro expands to
//! nothing and [`inv_fault!`] to `false`; without `antithesis-mutants`,
//! [`inv_mutant!`] expands to `false`.
//! Neither evaluates its arguments then, so an argument must not carry a side
//! effect the code relies on.

use std::fmt;

#[cfg(feature = "antithesis")]
#[doc(hidden)]
pub use antithesis_sdk as __sdk;

/// The environment variable that selects the one mutant a mutant image runs.
pub const MUTANT_ENV: &str = "RAVEL_ANTITHESIS_MUTANT";

/// The environment variable that sets the fault switches' rates, as
/// `name=rate,name=rate` with each rate a decimal in `[0, 1]`.
pub const FAULT_ENV: &str = "RAVEL_ANTITHESIS_FAULTS";

/// True exactly when the `inv_*` assertion macros evaluate their arguments.
pub const EVALUATES: bool = cfg!(any(feature = "antithesis", feature = "capture"));

/// Why [`init`] refused to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitError {
    /// `RAVEL_ANTITHESIS_MUTANT` (or a test's scoped selection) names no
    /// registered mutant.
    UnknownMutant { name: String },
    /// `RAVEL_ANTITHESIS_MUTANT` is not valid UTF-8.
    MutantNotUnicode,
    /// `RAVEL_ANTITHESIS_FAULTS` (or a test's scoped rate) names no
    /// registered fault.
    UnknownFault { name: String },
    /// An entry of `RAVEL_ANTITHESIS_FAULTS` is not `name=rate`.
    MalformedFaultEntry { entry: String },
    /// A fault's rate is not a decimal in `[0, 1]`.
    InvalidFaultRate { name: String, rate: String },
    /// `RAVEL_ANTITHESIS_FAULTS` names one fault twice.
    DuplicateFault { name: String },
    /// `RAVEL_ANTITHESIS_FAULTS` is not valid UTF-8.
    FaultsNotUnicode,
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
            InitError::UnknownFault { name } => write!(
                f,
                "{FAULT_ENV} names {name:?}, no registered fault; refusing to start \
                 (the registered names are in ravel_invariant::fault::FAULTS)"
            ),
            InitError::MalformedFaultEntry { entry } => write!(
                f,
                "{FAULT_ENV} entry {entry:?} is not name=rate; refusing to start"
            ),
            InitError::InvalidFaultRate { name, rate } => write!(
                f,
                "{FAULT_ENV} gives fault {name:?} the rate {rate:?}, not a decimal in \
                 [0, 1]; refusing to start"
            ),
            InitError::DuplicateFault { name } => write!(
                f,
                "{FAULT_ENV} names fault {name:?} more than once; refusing to start"
            ),
            InitError::FaultsNotUnicode => {
                write!(f, "{FAULT_ENV} is not valid UTF-8; refusing to start")
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
/// With `antithesis` or `capture` it reads `RAVEL_ANTITHESIS_FAULTS` once and
/// sets the fault rates for the life of the process; unset or empty enables
/// none, and a value `fault::parse_rates` refuses is the error returned.
/// With none of the three features it does nothing and reads nothing.
pub fn init() -> Result<(), InitError> {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();
    #[cfg(feature = "antithesis-mutants")]
    mutant::init_from_env()?;
    #[cfg(any(feature = "antithesis", feature = "capture"))]
    fault::init_from_env()?;
    Ok(())
}

#[cfg(any(
    feature = "antithesis",
    feature = "capture",
    feature = "antithesis-mutants"
))]
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

/// True when the named fault fires at this evaluation, drawn at the rate in
/// force for it: the rate a test set with `fault::scoped` on this thread,
/// else the one [`init`] read from `RAVEL_ANTITHESIS_FAULTS`. The name must
/// be in `fault::FAULTS`, which a build with `antithesis` or `capture`
/// checks at compile time. Without either feature this is the constant
/// `false`.
#[cfg(any(feature = "antithesis", feature = "capture"))]
#[macro_export]
macro_rules! inv_fault {
    ($name:literal) => {{
        const _: () = ::std::assert!(
            $crate::fault::is_registered($name),
            ::std::concat!(
                "inv_fault!: ",
                $name,
                " is not in ravel_invariant::fault::FAULTS"
            ),
        );
        $crate::fault::fires($name)
    }};
}
/// True when the named fault fires at this evaluation, drawn at the rate in
/// force for it: the rate a test set with `fault::scoped` on this thread,
/// else the one [`init`] read from `RAVEL_ANTITHESIS_FAULTS`. The name must
/// be in `fault::FAULTS`, which a build with `antithesis` or `capture`
/// checks at compile time. Without either feature this is the constant
/// `false`.
#[cfg(not(any(feature = "antithesis", feature = "capture")))]
#[macro_export]
macro_rules! inv_fault {
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

    /// Whether `name` is in [`MUTANTS`]; `inv_mutant!` asserts it at compile
    /// time.
    pub const fn is_registered(name: &str) -> bool {
        let mut i = 0;
        while i < MUTANTS.len() {
            if crate::str_eq(MUTANTS[i], name) {
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

/// The fault switches: the registered names, the process rates, the per-test
/// scope and the draw.
#[cfg(any(feature = "antithesis", feature = "capture"))]
pub mod fault {
    use std::cell::RefCell;
    use std::ffi::OsStr;
    use std::marker::PhantomData;
    use std::sync::OnceLock;

    use crate::{FAULT_ENV, InitError};

    /// Every fault `inv_fault!` may name, `<area>/<name>`.
    pub const FAULTS: &[&str] = &["ingest/encode-cancelled", "ingest/data-put-refused"];

    /// Whether `name` is in [`FAULTS`]; `inv_fault!` asserts it at compile
    /// time.
    pub const fn is_registered(name: &str) -> bool {
        let mut i = 0;
        while i < FAULTS.len() {
            if crate::str_eq(FAULTS[i], name) {
                return true;
            }
            i += 1;
        }
        false
    }

    fn registered(name: &str) -> Result<&'static str, InitError> {
        FAULTS
            .iter()
            .copied()
            .find(|f| *f == name)
            .ok_or_else(|| InitError::UnknownFault {
                name: name.to_owned(),
            })
    }

    fn parse_rate(name: &str, text: &str) -> Result<f64, InitError> {
        let invalid = || InitError::InvalidFaultRate {
            name: name.to_owned(),
            rate: text.to_owned(),
        };
        let bytes = text.as_bytes();
        let decimal = bytes.iter().all(|b| b.is_ascii_digit() || *b == b'.')
            && bytes.iter().filter(|b| **b == b'.').count() <= 1
            && bytes.iter().any(u8::is_ascii_digit);
        if !decimal {
            return Err(invalid());
        }
        let rate: f64 = text.parse().map_err(|_| invalid())?;
        if (0.0..=1.0).contains(&rate) {
            Ok(rate)
        } else {
            Err(invalid())
        }
    }

    /// The rates a value of `RAVEL_ANTITHESIS_FAULTS` sets: none when unset
    /// or empty; otherwise `name=rate` entries separated by `,`, each name in
    /// [`FAULTS`] at most once and each rate a decimal in `[0, 1]`. Any
    /// other value is an error.
    pub fn parse_rates(value: Option<&OsStr>) -> Result<Vec<(&'static str, f64)>, InitError> {
        let Some(value) = value else {
            return Ok(Vec::new());
        };
        let value = value.to_str().ok_or(InitError::FaultsNotUnicode)?;
        if value.is_empty() {
            return Ok(Vec::new());
        }
        let mut rates: Vec<(&'static str, f64)> = Vec::new();
        for entry in value.split(',') {
            let malformed = || InitError::MalformedFaultEntry {
                entry: entry.to_owned(),
            };
            let (name, rate) = entry.split_once('=').ok_or_else(malformed)?;
            if name.is_empty() {
                return Err(malformed());
            }
            let name = registered(name)?;
            let rate = parse_rate(name, rate)?;
            if rates.iter().any(|(seen, _)| *seen == name) {
                return Err(InitError::DuplicateFault {
                    name: name.to_owned(),
                });
            }
            rates.push((name, rate));
        }
        Ok(rates)
    }

    static PROCESS: OnceLock<Vec<(&'static str, f64)>> = OnceLock::new();

    pub(crate) fn init_from_env() -> Result<(), InitError> {
        let rates = parse_rates(std::env::var_os(FAULT_ENV).as_deref())?;
        // Read once: a second call keeps the first rates.
        let _ = PROCESS.set(rates);
        Ok(())
    }

    thread_local! {
        static SCOPED: RefCell<Option<Vec<(&'static str, f64)>>> = const { RefCell::new(None) };
    }

    fn rate_of(rates: &[(&'static str, f64)], name: &str) -> f64 {
        rates
            .iter()
            .find(|(n, _)| *n == name)
            .map_or(0.0, |(_, rate)| *rate)
    }

    /// The rate in force for `name` on this thread: the test scope's when one
    /// is open here, the process rates otherwise. A fault with no rate, and
    /// any fault before [`crate::init`], has rate 0.
    pub fn rate(name: &str) -> f64 {
        SCOPED
            .with(|scoped| scoped.borrow().as_deref().map(|r| rate_of(r, name)))
            .unwrap_or_else(|| PROCESS.get().map_or(0.0, |r| rate_of(r, name)))
    }

    /// One evaluation of `inv_fault!(name)`. Rate 0 returns false and rate 1
    /// returns true, neither with a draw; a rate between draws once. A firing
    /// records the reachable "fault: <name> fired".
    pub fn fires(name: &str) -> bool {
        let rate = rate(name);
        let fired = if rate <= 0.0 {
            false
        } else if rate >= 1.0 {
            true
        } else {
            unit_draw() < rate
        };
        if fired {
            record_fired(name);
        }
        fired
    }

    // One literal per fault: the SDK registers each reachable's message at
    // its site. `every_fault_records_its_firing` keeps this match and
    // `FAULTS` in step.
    fn record_fired(name: &str) {
        let details = std::collections::BTreeMap::from([("fault", name)]);
        match name {
            "ingest/encode-cancelled" => {
                crate::inv_reachable!("fault: ingest/encode-cancelled fired", details)
            }
            "ingest/data-put-refused" => {
                crate::inv_reachable!("fault: ingest/data-put-refused fired", details)
            }
            _ => {}
        }
    }

    /// A draw in `[0, 1)`.
    fn unit_draw() -> f64 {
        (random_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    #[cfg(feature = "antithesis")]
    fn random_u64() -> u64 {
        antithesis_sdk::random::get_random()
    }

    // splitmix64 over a per-thread seed from std's randomly keyed hasher.
    #[cfg(not(feature = "antithesis"))]
    fn random_u64() -> u64 {
        use std::cell::Cell;
        use std::hash::{BuildHasher, RandomState};

        thread_local! {
            static STATE: Cell<u64> =
                Cell::new(RandomState::new().hash_one(std::thread::current().id()));
        }
        STATE.with(|state| {
            let next = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
            state.set(next);
            let mut z = next;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        })
    }

    /// Sets `name`'s rate on the calling thread until the guard drops, for a
    /// test. A thread with a scope open sees only the rates its open scopes
    /// set, not the process rates, so tests in one binary do not see each
    /// other's faults. Code the test hands to another thread (a multi-thread
    /// runtime's workers, the blocking pool) does not see it either: drive
    /// the fault site on the test's own thread.
    pub fn scoped(name: &str, rate: f64) -> Result<ScopedFault, InitError> {
        let name = registered(name)?;
        if !(0.0..=1.0).contains(&rate) {
            return Err(InitError::InvalidFaultRate {
                name: name.to_owned(),
                rate: rate.to_string(),
            });
        }
        let previous = SCOPED.with(|scoped| {
            let mut scoped = scoped.borrow_mut();
            let previous = scoped.clone();
            let rates = scoped.get_or_insert_with(Vec::new);
            rates.retain(|(n, _)| *n != name);
            rates.push((name, rate));
            previous
        });
        Ok(ScopedFault {
            previous,
            _not_send: PhantomData,
        })
    }

    /// Restores the calling thread's previous scope on drop.
    ///
    /// It is not `Send`: dropped on another thread it would restore that
    /// thread's slot and leave the scope open on this one.
    #[must_use = "the fault's scoped rate ends when this guard drops"]
    pub struct ScopedFault {
        previous: Option<Vec<(&'static str, f64)>>,
        _not_send: PhantomData<*const ()>,
    }

    impl Drop for ScopedFault {
        fn drop(&mut self) {
            let previous = self.previous.take();
            SCOPED.with(|scoped| *scoped.borrow_mut() = previous);
        }
    }

    // Fails to compile if ScopedFault becomes Send: the call below is then
    // ambiguous between the two impls.
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    struct IsSend;
    impl<T: ?Sized + Send> AmbiguousIfSend<IsSend> for T {}
    const _: fn() = || <ScopedFault as AmbiguousIfSend<_>>::check();
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

    #[cfg(not(any(feature = "antithesis", feature = "capture")))]
    #[test]
    fn inv_fault_is_false_without_the_feature() {
        for _ in 0..1_000 {
            assert!(!inv_fault!("ingest/encode-cancelled"));
            assert!(!inv_fault!("ingest/data-put-refused"));
        }
    }

    #[cfg(any(feature = "antithesis", feature = "capture"))]
    #[test]
    fn fault_values_init_refuses_have_typed_errors() {
        use std::ffi::OsStr;

        use crate::fault::{parse_rates, scoped};
        use crate::{FAULT_ENV, InitError};

        let parse = |value: &str| parse_rates(Some(OsStr::new(value)));
        let unknown = InitError::UnknownFault {
            name: "ingest/no-such-fault".to_owned(),
        };
        assert_eq!(parse("ingest/no-such-fault=1"), Err(unknown.clone()));
        assert_eq!(
            scoped("ingest/no-such-fault", 1.0).err(),
            Some(unknown.clone()),
            "a test scope refuses the same names"
        );
        assert_eq!(
            parse("ingest/encode-cancelled=1.5"),
            Err(InitError::InvalidFaultRate {
                name: "ingest/encode-cancelled".to_owned(),
                rate: "1.5".to_owned(),
            })
        );
        assert_eq!(
            scoped("ingest/encode-cancelled", 1.5).err(),
            Some(InitError::InvalidFaultRate {
                name: "ingest/encode-cancelled".to_owned(),
                rate: "1.5".to_owned(),
            })
        );
        for rate in ["", "-0.5", "nan", "inf", "1e-1", "0.5.0", ".", "half"] {
            assert_eq!(
                parse(&format!("ingest/encode-cancelled={rate}")),
                Err(InitError::InvalidFaultRate {
                    name: "ingest/encode-cancelled".to_owned(),
                    rate: rate.to_owned(),
                }),
                "rate {rate:?}"
            );
        }
        for entry in ["ingest/encode-cancelled", "=1"] {
            assert_eq!(
                parse(entry),
                Err(InitError::MalformedFaultEntry {
                    entry: entry.to_owned()
                }),
                "entry {entry:?}"
            );
        }
        assert_eq!(
            parse("ingest/data-put-refused=1,"),
            Err(InitError::MalformedFaultEntry {
                entry: String::new()
            }),
            "a trailing separator is an empty entry"
        );
        let duplicate = InitError::DuplicateFault {
            name: "ingest/data-put-refused".to_owned(),
        };
        assert_eq!(
            parse("ingest/data-put-refused=0.5,ingest/data-put-refused=0.5"),
            Err(duplicate.clone())
        );
        for err in [
            unknown,
            duplicate,
            InitError::MalformedFaultEntry { entry: "x".into() },
            InitError::InvalidFaultRate {
                name: "x".into(),
                rate: "2".into(),
            },
            InitError::FaultsNotUnicode,
        ] {
            let text = err.to_string();
            assert!(
                text.contains(FAULT_ENV) && text.contains("refusing to start"),
                "{text}"
            );
        }
    }

    #[cfg(all(any(feature = "antithesis", feature = "capture"), unix))]
    #[test]
    fn non_unicode_fault_value_is_refused() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        assert_eq!(
            crate::fault::parse_rates(Some(OsStr::from_bytes(b"ingest/\xff=1"))),
            Err(crate::InitError::FaultsNotUnicode)
        );
    }

    #[cfg(any(feature = "antithesis", feature = "capture"))]
    #[test]
    fn a_valid_fault_value_enables_exactly_the_named_faults() {
        use std::ffi::OsStr;

        use crate::fault::parse_rates;

        assert_eq!(parse_rates(None), Ok(Vec::new()));
        assert_eq!(parse_rates(Some(OsStr::new(""))), Ok(Vec::new()));
        assert_eq!(
            parse_rates(Some(OsStr::new("ingest/data-put-refused=0.25"))),
            Ok(vec![("ingest/data-put-refused", 0.25)])
        );
        assert_eq!(
            parse_rates(Some(OsStr::new(
                "ingest/encode-cancelled=1,ingest/data-put-refused=0"
            ))),
            Ok(vec![
                ("ingest/encode-cancelled", 1.0),
                ("ingest/data-put-refused", 0.0)
            ])
        );
        assert_eq!(
            parse_rates(Some(OsStr::new("ingest/encode-cancelled=.5"))),
            Ok(vec![("ingest/encode-cancelled", 0.5)])
        );
    }

    #[cfg(any(feature = "antithesis", feature = "capture"))]
    #[test]
    fn rate_zero_never_fires_and_rate_one_always_fires() {
        use crate::fault::scoped;

        const DRAWS: usize = 10_000;
        let count = || {
            (0..DRAWS)
                .filter(|_| inv_fault!("ingest/encode-cancelled"))
                .count()
        };
        assert_eq!(count(), 0, "no scope and no process rate: rate 0");
        {
            let _off = scoped("ingest/encode-cancelled", 0.0).expect("registered");
            assert_eq!(count(), 0, "rate 0 never fires");
        }
        {
            let _on = scoped("ingest/encode-cancelled", 1.0).expect("registered");
            assert_eq!(count(), DRAWS, "rate 1 always fires");
            assert!(
                !inv_fault!("ingest/data-put-refused"),
                "a scope sets only the fault it names"
            );
        }
        {
            let _half = scoped("ingest/encode-cancelled", 0.5).expect("registered");
            let fired = count();
            assert!(
                (4_000..=6_000).contains(&fired),
                "rate 0.5 fired {fired} of {DRAWS}"
            );
        }
        assert_eq!(count(), 0, "the scope ended with its guard");
    }

    #[cfg(feature = "capture")]
    #[test]
    fn every_fault_records_its_firing() {
        use crate::capture::{Kind, evaluations_of};

        for name in crate::fault::FAULTS.iter().copied() {
            let message = format!("fault: {name} fired");
            let before = evaluations_of(&message).len();
            let guard = crate::fault::scoped(name, 1.0).expect("registered");
            assert!(crate::fault::fires(name));
            drop(guard);
            assert!(!crate::fault::fires(name));
            let after = evaluations_of(&message);
            assert_eq!(after.len(), before + 1, "{message}: {after:?}");
            let last = after.last().expect("one evaluation");
            assert_eq!(last.kind, Kind::Reachable);
            assert!(last.details.contains(name), "{}", last.details);
        }
    }

    // Two tests in one binary, run as two threads that each set a different
    // fault. Both hold their scope before either reads and until both have
    // read, so a process-wide rate would show one of them the other's fault.
    #[cfg(any(feature = "antithesis", feature = "capture"))]
    #[test]
    fn scoped_faults_do_not_leak_between_tests() {
        use std::sync::{Arc, Barrier};

        use crate::fault::scoped;

        let both_set = Arc::new(Barrier::new(2));
        let both_read = Arc::new(Barrier::new(2));
        let run = |name: &'static str| {
            let both_set = Arc::clone(&both_set);
            let both_read = Arc::clone(&both_read);
            std::thread::spawn(move || {
                let guard = scoped(name, 1.0).expect("a registered fault");
                both_set.wait();
                let seen = (
                    inv_fault!("ingest/encode-cancelled"),
                    inv_fault!("ingest/data-put-refused"),
                );
                both_read.wait();
                drop(guard);
                let after = (
                    inv_fault!("ingest/encode-cancelled"),
                    inv_fault!("ingest/data-put-refused"),
                );
                (seen, after)
            })
        };
        let first = run("ingest/encode-cancelled");
        let second = run("ingest/data-put-refused");
        let (first, second) = (
            first.join().expect("first test thread"),
            second.join().expect("second test thread"),
        );
        assert_eq!(first, ((true, false), (false, false)), "first test's view");
        assert_eq!(
            second,
            ((false, true), (false, false)),
            "second test's view"
        );
        assert!(
            !inv_fault!("ingest/encode-cancelled") && !inv_fault!("ingest/data-put-refused"),
            "a thread that opened no scope sees no fault"
        );
    }

    #[cfg(not(any(feature = "antithesis", feature = "capture")))]
    #[test]
    fn evaluates_is_false_without_the_feature() {
        let evaluated = std::cell::Cell::new(false);
        inv_always!(
            {
                evaluated.set(true);
                true
            },
            "ravel-invariant test: evaluates is false without the feature",
            serde_json::json!({})
        );
        assert!(!evaluated.get());
        assert_eq!(evaluated.get(), crate::EVALUATES);
    }

    #[cfg(feature = "capture")]
    #[test]
    fn evaluates_is_true_under_capture() {
        let evaluated = std::cell::Cell::new(false);
        inv_always!(
            {
                evaluated.set(true);
                true
            },
            "ravel-invariant test: evaluates is true under capture",
            serde_json::json!({})
        );
        assert!(evaluated.get());
        assert_eq!(evaluated.get(), crate::EVALUATES);
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
