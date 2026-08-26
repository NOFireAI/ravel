//! RLOG version 4: row groups with column-major page placement and the
//! PAGE_DIR section (ADR-0699).
//!
//! Covers the acceptance anchors ADR-0699 names: a differential check that
//! a version-4 object decodes to the same records as the version-3 writer
//! produced for the same batch, projection pushdown through PAGE_DIR, the
//! row-group boundary cases, and the corrupt-input cases (a flipped page
//! byte, a flipped PAGE_DIR byte, a truncated last row group).
