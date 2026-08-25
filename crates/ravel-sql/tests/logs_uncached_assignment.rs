//! Tests for issue #693: un-cached logs scan assigns whole segments to
//! partitions instead of striping every segment's blocks across all
//! partitions.
