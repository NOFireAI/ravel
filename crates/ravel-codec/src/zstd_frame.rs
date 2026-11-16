//! zstd frame decompression through a per-thread reused decompression
//! context. `zstd::bulk::decompress` creates and frees a `ZSTD_DCtx` on every
//! call, which costs more than decoding a small page; read paths decode many
//! such frames back to back, so they go through [`decompress`] instead.

use std::cell::RefCell;
use std::io;

use zstd::bulk::Decompressor;

thread_local! {
    static DCTX: RefCell<Option<Decompressor<'static>>> = const { RefCell::new(None) };
}

/// Decompresses one zstd frame into a fresh `Vec` of exactly `capacity`
/// bytes of capacity. Output, bound and errors are those of
/// `zstd::bulk::decompress(src, capacity)`: a frame that decodes to more than
/// `capacity` bytes, or a truncated or corrupt frame, is an `Err`.
///
/// Each frame starts from a reset context, so an earlier call (including a
/// failed one) on the same thread does not affect this one. If the
/// thread-local context is unavailable (re-entered, or the thread is tearing
/// down), this falls back to a one-shot context.
pub fn decompress(src: &[u8], capacity: usize) -> io::Result<Vec<u8>> {
    DCTX.try_with(|cell| {
        let Ok(mut slot) = cell.try_borrow_mut() else {
            return zstd::bulk::decompress(src, capacity);
        };
        let dctx = match &mut *slot {
            Some(dctx) => dctx,
            None => slot.insert(Decompressor::new()?),
        };
        dctx.decompress(src, capacity)
    })
    .unwrap_or_else(|_| zstd::bulk::decompress(src, capacity))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| ((i / 5) as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    #[test]
    fn consecutive_decodes_of_different_sizes_are_exact() {
        let big = payload(70_000, 1);
        let small = payload(900, 2);
        let mid = payload(9_000, 3);
        let zbig = zstd::bulk::compress(&big, 3).expect("compress");
        let zsmall = zstd::bulk::compress(&small, 3).expect("compress");
        let zmid = zstd::bulk::compress(&mid, 3).expect("compress");

        assert_eq!(decompress(&zsmall, small.len()).expect("small"), small);
        assert_eq!(decompress(&zbig, big.len()).expect("big"), big);
        assert_eq!(
            decompress(&zsmall, small.len()).expect("small again"),
            small
        );
        // A truncated frame leaves a streaming context mid-frame; the next
        // frame must still decode from a clean state.
        assert!(decompress(&zbig[..zbig.len() / 2], big.len()).is_err());
        assert_eq!(decompress(&zmid, mid.len()).expect("mid"), mid);
    }

    #[test]
    fn capacity_bound_is_per_call() {
        let small = payload(600, 4);
        let big = payload(50_000, 5);
        let zsmall = zstd::bulk::compress(&small, 3).expect("compress");
        let zbig = zstd::bulk::compress(&big, 3).expect("compress");

        assert_eq!(decompress(&zsmall, small.len()).expect("small"), small);
        // A later call with a larger capacity is not held to the first call's.
        assert_eq!(decompress(&zbig, big.len()).expect("big"), big);
        // And a later call with a smaller capacity is held to its own.
        assert!(decompress(&zbig, big.len() - 1).is_err());
        assert_eq!(decompress(&zsmall, small.len()).expect("small"), small);
    }

    #[test]
    fn corrupt_and_truncated_frames_error_through_the_reused_context() {
        let data = payload(20_000, 6);
        let z = zstd::bulk::compress(&data, 3).expect("compress");
        assert_eq!(decompress(&z, data.len()).expect("warm"), data);

        assert!(decompress(&z[..z.len() - 3], data.len()).is_err());
        let mut corrupt = z.clone();
        corrupt[0] ^= 0xff;
        assert!(decompress(&corrupt, data.len()).is_err());

        assert_eq!(decompress(&z, data.len()).expect("after errors"), data);
    }
}
