use super::*;
use crate::head::tests::{head, sample_head};
use crate::head::{InputSource, ResultStatus};
use proptest::collection::vec;
use proptest::prelude::*;

fn inbox_entry(accept_seq: u64) -> InboxEntry {
    InboxEntry {
        accept_seq,
        handler: "h".to_string(),
        source: InputSource::Message {
            request_id: format!("r{accept_seq}"),
            request_hash: crate::identity::Hash256([1; 32]),
        },
        payload: Some(ObjectRef::of(&accept_seq.to_be_bytes())),
        accepted_unix_ns: 0,
        failed_attempts: 0,
        last_failed_turn_id: None,
    }
}

fn timer(generation: u64, status: TimerStatus) -> TimerEntry {
    TimerEntry {
        timer_id: Id128([generation as u8; 16]),
        timer_key: format!("t{generation}"),
        generation,
        due_unix_ns: 0,
        handler: "h".to_string(),
        payload: None,
        status,
    }
}

fn bounds(k: usize) -> PageBounds {
    PageBounds {
        max_page_entries: k,
        max_index_pages: 512,
    }
}

/// `Limits::max_page_entries` bounds a page: the writer takes exactly that
/// many entries and refuses the next one typed, and the seal rule opens a new
/// page only on the publish that takes the open page past it.
#[test]
fn a_full_page_refuses_the_next_append_typed() {
    const K: usize = 4;
    let mut writer = PageWriter::new(K);
    for seq in 1..=K as u64 {
        writer.push(inbox_entry(seq)).expect("fits");
    }
    assert_eq!(
        writer.push(inbox_entry(K as u64 + 1)),
        Err(PageError::PageFull { max: K })
    );
    assert_eq!(writer.len(), K);

    // Exactly K entries stay open; the publish that adds the K+1st seals a
    // page of K and leaves the new entry as the open page.
    let full: Vec<InboxEntry> = (1..=K as u64).map(inbox_entry).collect();
    let (dir, written) =
        seal(&Directory::default(), &full, u64::MAX, bounds(K)).expect("seals");
    assert!(dir.pages.is_empty());
    assert!(written.is_empty());
    let next: Vec<InboxEntry> = (1..=K as u64 + 1).map(inbox_entry).collect();
    let (dir, written) = seal(&dir, &next, u64::MAX, bounds(K)).expect("seals");
    assert_eq!(dir.pages.len(), 1);
    assert_eq!(dir.pages[0].entries, K as u32);
    assert_eq!(dir.tail_from, K as u64 + 1);
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].page, dir.pages[0].page);
    assert_eq!(
        InboxEntry::decode_page(&written[0].bytes),
        Ok(full.clone())
    );
}

/// `Limits::max_index_pages` bounds a directory: the publish that would seal
/// one page past it is refused typed.
#[test]
fn a_full_directory_refuses_the_next_page_typed() {
    let entries: Vec<InboxEntry> = (1..=9).map(inbox_entry).collect();
    let tight = PageBounds {
        max_page_entries: 4,
        max_index_pages: 1,
    };
    assert_eq!(
        seal(&Directory::default(), &entries, u64::MAX, tight),
        Err(PageError::DirectoryFull {
            list: ListKind::Inbox,
            max: 1,
        })
    );
}

/// An inline (version 1) list moves to paged in one seal, every page sealed
/// in that one call.
#[test]
fn an_inline_list_is_paged_in_one_seal() {
    let entries: Vec<InboxEntry> = (1..=10).map(inbox_entry).collect();
    let inline = Directory {
        representation: Representation::Inline,
        ..Directory::default()
    };
    let (dir, written) = seal(&inline, &entries, u64::MAX, bounds(4)).expect("seals");
    assert_eq!(dir.representation, Representation::Paged);
    assert_eq!(dir.pages.len(), 2);
    assert_eq!(written.len(), 2);
    assert_eq!(dir.tail_from, 9);
}

/// Re-sealing an unchanged loaded page writes nothing; a page not in memory
/// stays a reference, and an entry that falls in it is refused `NotLoaded`.
#[test]
fn a_reseal_writes_only_changed_pages() {
    let entries: Vec<InboxEntry> = (1..=6).map(inbox_entry).collect();
    let (dir, _) = seal(&Directory::default(), &entries, u64::MAX, bounds(4)).expect("seals");
    let (again, written) = seal(&dir, &entries, u64::MAX, bounds(4)).expect("seals");
    assert_eq!(again, dir);
    assert!(written.is_empty());

    let mut shallow = dir.clone();
    shallow.loaded = vec![false];
    let open: Vec<InboxEntry> = entries[4..].to_vec();
    let (kept, written) = seal(&shallow, &open, u64::MAX, bounds(4)).expect("seals");
    assert_eq!(kept, dir);
    assert_eq!(kept.loaded, vec![false]);
    assert!(written.is_empty());
    assert_eq!(
        seal(&shallow, &entries, u64::MAX, bounds(4)),
        Err(PageError::NotLoaded {
            list: ListKind::Inbox,
            page: 0,
        })
    );
}

/// The inbox's open page is never empty while a sealed page exists: removing
/// the last open input pulls the last sealed page back inline, so a HEAD
/// read without its pages still answers whether the inbox is empty.
#[test]
fn an_empty_open_inbox_pulls_the_last_page_back() {
    let entries: Vec<InboxEntry> = (1..=9).map(inbox_entry).collect();
    let (dir, _) = seal(&Directory::default(), &entries, u64::MAX, bounds(4)).expect("seals");
    assert_eq!(dir.pages.len(), 2);
    let remaining: Vec<InboxEntry> = entries[..8].to_vec();
    let (dir, written) = seal(&dir, &remaining, u64::MAX, bounds(4)).expect("seals");
    assert_eq!(dir.pages.len(), 1);
    assert_eq!(dir.tail_from, 5);
    assert!(written.is_empty());
    let first: Vec<InboxEntry> = entries[..4].to_vec();
    let (dir, _) = seal(&dir, &first, u64::MAX, bounds(4)).expect("seals");
    assert_eq!(dir, Directory::default());
}

/// A sealed timer page never holds a scheduled timer: timers seal only below
/// the lowest scheduled generation, so wake and `FireTimer` read no timer
/// page.
#[test]
fn timer_pages_never_hold_a_scheduled_timer() {
    let mut head = sample_head();
    head.index = Index::default();
    head.timers = (1..=12)
        .map(|g| {
            let status = if g == 3 || g >= 11 {
                TimerStatus::Scheduled
            } else {
                TimerStatus::Delivered
            };
            timer(g, status)
        })
        .collect();
    seal_index(&mut head, bounds(2)).expect("seals");
    let dir = &head.index.timers;
    assert_eq!(dir.pages.len(), 1);
    assert_eq!(dir.tail_from, 3);
    assert!(dir.pages.iter().all(|p| p.marked == 0));
    assert!(
        head.timers
            .iter()
            .filter(|t| t.marked())
            .all(|t| dir.is_inline(t.generation))
    );

    // Generation 3 settles: everything below 11 may now seal.
    if let Some(t) = head.timers.iter_mut().find(|t| t.generation == 3) {
        t.status = TimerStatus::Cancelled;
    }
    seal_index(&mut head, bounds(2)).expect("seals");
    let dir = &head.index.timers;
    assert_eq!(dir.pages.len(), 5);
    assert!(dir.pages.iter().all(|p| p.marked == 0));
    assert_eq!(dir.tail_from, 11);
}

/// Results seal only below the lowest pending input, whose result a later
/// publish appends.
#[test]
fn results_seal_below_the_lowest_pending_input() {
    let mut head = sample_head();
    head.index = Index::default();
    head.active_turn = None;
    head.inbox = vec![inbox_entry(4)];
    head.results = [1, 2, 3, 5, 6, 7, 8]
        .into_iter()
        .map(|accept_seq| ResultEntry {
            accept_seq,
            status: ResultStatus::Completed,
            output: None,
            error: String::new(),
            turn_id: None,
            dead_lettered: false,
            finished_unix_ns: 0,
        })
        .collect();
    seal_index(&mut head, bounds(2)).expect("seals");
    assert_eq!(head.index.results.pages.len(), 1);
    assert!(head.index.results.tail_from <= 4);
}

/// `attach_page` takes only the bytes HEAD records for that page.
#[test]
fn attach_refuses_bytes_that_are_not_the_recorded_page() {
    let mut head = sample_head();
    head.index = Index::default();
    head.inbox = (1..=6).map(inbox_entry).collect();
    seal_index(&mut head, bounds(4)).expect("seals");
    let page = head.index.written[0].clone();
    let mut shallow = head.clone();
    shallow.inbox.retain(|e| e.accept_seq > 4);
    shallow.index.inbox.loaded = vec![false];

    let mut other = page.bytes.clone();
    other.push(0);
    assert_eq!(
        attach_page(&mut shallow.clone(), ListKind::Inbox, 0, &other),
        Err(CodecError::Invalid { field: "page.hash" })
    );
    assert_eq!(
        attach_page(&mut shallow.clone(), ListKind::Inbox, 1, &page.bytes),
        Err(CodecError::Invalid {
            field: "page.index"
        })
    );
    assert_eq!(
        attach_page(&mut shallow.clone(), ListKind::Receipts, 0, &page.bytes),
        Err(CodecError::Invalid {
            field: "page.index"
        })
    );
    attach_page(&mut shallow, ListKind::Inbox, 0, &page.bytes).expect("attaches");
    assert_eq!(shallow.inbox, head.inbox);
    assert!(shallow.index.inbox.is_loaded(0));
    attach_page(&mut shallow, ListKind::Inbox, 0, &page.bytes).expect("idempotent");
    assert_eq!(shallow.inbox, head.inbox);
}

fn object_ref() -> impl Strategy<Value = ObjectRef> {
    (any::<[u8; 32]>(), any::<u64>()).prop_map(|(h, size_bytes)| ObjectRef {
        hash: ObjectHash(h),
        size_bytes,
    })
}

fn page_meta(first_key: u64) -> impl Strategy<Value = PageMeta> {
    (
        object_ref(),
        any::<u32>(),
        any::<u32>(),
        any::<i64>(),
        any::<u64>(),
        vec(any::<u32>(), 0..6),
    )
        .prop_map(
            move |(page, entries, marked, min_due_unix_ns, referenced_bytes, mut fps)| {
                fps.sort_unstable();
                PageMeta {
                    first_key,
                    page,
                    entries,
                    marked,
                    min_due_unix_ns,
                    referenced_bytes,
                    fingerprints: fps,
                }
            },
        )
}

/// A paged directory: `first_key`s strictly ascending, `tail_from` above the
/// last, or no page at all.
fn directory() -> impl Strategy<Value = Directory> {
    (any::<u32>(), vec(1u64..1_000, 0..4), 1u64..1_000).prop_flat_map(|(start, gaps, tail)| {
        let mut keys = Vec::with_capacity(gaps.len());
        let mut key = u64::from(start);
        for gap in gaps {
            keys.push(key);
            key += gap;
        }
        let tail_from = if keys.is_empty() { 0 } else { key + tail };
        keys.into_iter()
            .map(page_meta)
            .collect::<Vec<_>>()
            .prop_map(move |pages| Directory {
                representation: Representation::Paged,
                loaded: vec![false; pages.len()],
                pages,
                tail_from,
            })
    })
}

/// A HEAD with a paged directory on every list and its inline entries
/// outside every sealed range, as the decoder requires.
fn paged_head() -> impl Strategy<Value = Head> {
    (head(), [directory(), directory(), directory(), directory(), directory()]).prop_map(
        |(mut h, [inbox, receipts, results, events, timers])| {
            h.inbox.retain(|e| inbox.is_inline(e.accept_seq));
            h.receipts.retain(|e| receipts.is_inline(e.accept_seq));
            h.results.retain(|e| results.is_inline(e.accept_seq));
            h.events.retain(|e| events.is_inline(e.event_seq));
            h.timers.retain(|e| timers.is_inline(e.generation));
            h.index = Index {
                inbox,
                receipts,
                results,
                events,
                timers,
                written: Vec::new(),
            };
            h
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5_000))]

    #[test]
    fn paged_heads_round_trip(h in paged_head()) {
        let bytes = h.encode();
        prop_assert_eq!(crate::head::peek_format_version(&bytes), Ok(crate::head::FORMAT_VERSION));
        let decoded = Head::decode(&bytes);
        prop_assert_eq!(decoded.as_ref(), Ok(&h));
        prop_assert_eq!(decoded.map(|d| d.encode()), Ok(bytes));
    }

    #[test]
    fn every_strict_prefix_of_a_paged_head_is_a_typed_error(
        h in paged_head(),
        cut in any::<prop::sample::Index>(),
    ) {
        let bytes = h.encode();
        let len = cut.index(bytes.len());
        prop_assert!(Head::decode(&bytes[..len]).is_err());
    }

    #[test]
    fn corrupt_paged_heads_never_panic(
        h in paged_head(),
        flips in vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
    ) {
        let mut bytes = h.encode();
        for (at, value) in flips {
            let i = at.index(bytes.len());
            bytes[i] = value;
        }
        let _ = Head::decode(&bytes);
    }

    /// Any list split by the seal rule reads back, page by page, as the list
    /// it was sealed from.
    #[test]
    fn sealed_pages_attach_back_to_the_list(n in 0u64..40, k in 1usize..6) {
        let mut head = sample_head();
        head.index = Index::default();
        head.inbox = (1..=n).map(inbox_entry).collect();
        seal_index(&mut head, bounds(k)).expect("seals");
        let full = head.inbox.clone();
        let dir = head.index.inbox.clone();
        prop_assert_eq!(dir.pages.len(), (n.saturating_sub(1) / k as u64) as usize);
        prop_assert!(dir.pages.iter().all(|p| p.entries as usize == k));
        let mut shallow = Head::decode(&head.encode()).expect("decodes");
        for (i, w) in head.index.written.iter().enumerate() {
            attach_page(&mut shallow, ListKind::Inbox, i, &w.bytes).expect("attaches");
        }
        prop_assert_eq!(shallow.inbox, full);
    }
}
