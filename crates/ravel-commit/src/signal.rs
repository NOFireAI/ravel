//! Conversions between [`ravel_types::Signal`] and the persistent
//! `ravel.commit.v1.Signal` protobuf enum, plus the single-letter key-path
//! prefix. Used by `keys` and `record`, and public so `ravel-catalog` can
//! validate a decoded record's `signal` field against the expected query
//! signal without re-deriving the mapping.

use ravel_types::Signal;

/// A signal value could not be recognized: an unknown key-path prefix
/// letter, or an out-of-range/unspecified protobuf enum value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownSignal;

pub fn to_proto(signal: Signal) -> ravel_proto::commit::v1::Signal {
    match signal {
        Signal::Metrics => ravel_proto::commit::v1::Signal::Metrics,
        Signal::Logs => ravel_proto::commit::v1::Signal::Logs,
        Signal::Spans => ravel_proto::commit::v1::Signal::Spans,
        Signal::Profiles => ravel_proto::commit::v1::Signal::Profiles,
    }
}

pub fn from_proto(raw: i32) -> Result<Signal, UnknownSignal> {
    match ravel_proto::commit::v1::Signal::try_from(raw).map_err(|_| UnknownSignal)? {
        ravel_proto::commit::v1::Signal::Unspecified => Err(UnknownSignal),
        ravel_proto::commit::v1::Signal::Metrics => Ok(Signal::Metrics),
        ravel_proto::commit::v1::Signal::Logs => Ok(Signal::Logs),
        ravel_proto::commit::v1::Signal::Spans => Ok(Signal::Spans),
        ravel_proto::commit::v1::Signal::Profiles => Ok(Signal::Profiles),
    }
}

pub fn from_prefix(prefix: &str) -> Result<Signal, UnknownSignal> {
    match prefix {
        "m" => Ok(Signal::Metrics),
        "l" => Ok(Signal::Logs),
        "s" => Ok(Signal::Spans),
        "p" => Ok(Signal::Profiles),
        _ => Err(UnknownSignal),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn proto_round_trips_for_every_signal() {
        for signal in [
            Signal::Metrics,
            Signal::Logs,
            Signal::Spans,
            Signal::Profiles,
        ] {
            let proto = to_proto(signal);
            assert_eq!(from_proto(proto as i32).expect("known"), signal);
            let prefix = signal.key_prefix();
            assert_eq!(from_prefix(prefix).expect("known"), signal);
        }
    }

    #[test]
    fn unspecified_and_unknown_are_rejected() {
        assert_eq!(
            from_proto(ravel_proto::commit::v1::Signal::Unspecified as i32),
            Err(UnknownSignal)
        );
        assert_eq!(from_proto(999), Err(UnknownSignal));
        assert_eq!(from_prefix("x"), Err(UnknownSignal));
    }
}
