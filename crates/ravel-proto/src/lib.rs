//! Generated protobuf types for persistent metadata. The .proto files under
//! `proto/ravel/` are the source of truth; field numbers are frozen.

pub mod segment {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/ravel.segment.v1.rs"));
    }
}

pub mod commit {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/ravel.commit.v1.rs"));
    }
}

pub mod catalog {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/ravel.catalog.v1.rs"));
    }
}
