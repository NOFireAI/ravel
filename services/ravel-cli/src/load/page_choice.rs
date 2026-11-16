//! `ravel-cli load --page-choice` (issue #4212, ADR-4203 decisions 6 and 7):
//! how the RLOG writer of every flush a logs load runs picks each page's
//! encoding.

use serde::{Deserialize, Serialize};

/// The page choice a logs load writes its RLOG objects with, the router's
/// [`ravel_ingest::IngestConfig::rlog_page_choice`]. Both values write
/// encodings the format defines, and readers decode either the same way.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoadPageChoice {
    /// Seal every candidate encoding of each page and keep the one with the
    /// fewest stored bytes.
    Exhaustive,
    /// Seal one candidate per page, picked from the page's statistics:
    /// cheaper to write, slightly larger objects.
    #[default]
    Estimated,
}

impl LoadPageChoice {
    /// The writer setting this value selects.
    pub fn page_choice(self) -> ravel_logseg::PageChoice {
        match self {
            Self::Exhaustive => ravel_logseg::PageChoice::Exhaustive,
            Self::Estimated => ravel_logseg::PageChoice::Estimated,
        }
    }

    /// The value as `--page-choice` spells it, which the summary and the
    /// report JSON print.
    pub fn name(self) -> &'static str {
        match self {
            Self::Exhaustive => "exhaustive",
            Self::Estimated => "estimated",
        }
    }
}
