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
    let (dir, written) = seal(&Directory::default(), &full, u64::MAX, bounds(K)).expect("seals");
    assert!(dir.pages.is_empty());
    assert!(written.is_empty());
    let next: Vec<InboxEntry> = (1..=K as u64 + 1).map(inbox_entry).collect();
    let (dir, written) = seal(&dir, &next, u64::MAX, bounds(K)).expect("seals");
    assert_eq!(dir.pages.len(), 1);
    assert_eq!(dir.pages[0].entries, K as u32);
    assert_eq!(dir.tail_from, K as u64 + 1);
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].page, dir.pages[0].page);
    assert_eq!(InboxEntry::decode_page(&written[0].bytes), Ok(full.clone()));
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

fn event(event_seq: u64, accept_seq: u64) -> EventEntry {
    EventEntry {
        event_seq,
        accept_seq,
        turn_id: Id128([accept_seq as u8; 16]),
        event_type: "e".to_string(),
        payload: None,
    }
}

/// An events page records the lowest and highest `accept_seq` it holds, not
/// its first one: an audit event that carries an older input widens the
/// range downward. Pages of the other lists record none.
#[test]
fn an_events_page_records_the_accept_range_of_its_events() {
    let mut head = sample_head();
    head.index = Index::default();
    // Page 1: operations 3 and 4. Page 2: operation 5, then an audit event
    // for operation 2, then operation 6. The open page: operation 7.
    head.events = [
        (1, 3),
        (2, 3),
        (3, 4),
        (4, 4),
        (5, 5),
        (6, 2),
        (7, 6),
        (8, 6),
        (9, 7),
    ]
    .into_iter()
    .map(|(seq, accept)| event(seq, accept))
    .collect();
    head.inbox = (1..=6).map(inbox_entry).collect();
    seal_index(&mut head, bounds(4)).expect("seals");
    let ranges: Vec<(u64, u64)> = head
        .index
        .events
        .pages
        .iter()
        .map(|p| (p.min_accept_seq, p.max_accept_seq))
        .collect();
    assert_eq!(ranges, [(3, 4), (2, 6)]);
    let holding = |seq: u64| -> Vec<usize> {
        let pages = &head.index.events.pages;
        (0..pages.len())
            .filter(|&i| pages[i].may_hold_operation(seq))
            .collect()
    };
    assert_eq!(holding(1), Vec::<usize>::new());
    assert_eq!(holding(2), [1]);
    assert_eq!(holding(3), [0, 1]);
    assert_eq!(holding(7), Vec::<usize>::new());
    assert_eq!(head.index.inbox.pages.len(), 1);
    let inbox = &head.index.inbox.pages[0];
    assert_eq!((inbox.min_accept_seq, inbox.max_accept_seq), (0, 0));
    let decoded = Head::decode(&head.encode()).expect("decodes");
    assert_eq!(decoded.index, head.index);
}

/// A page reference whose `accept_seq` range is inverted, or that records one
/// on a list other than events, is refused on decode; and an events page
/// whose entries do not span the recorded range is refused on attach.
#[test]
fn an_inverted_or_misplaced_accept_range_is_refused() {
    let mut head = sample_head();
    head.index = Index::default();
    head.events = (1..=6).map(|seq| event(seq, seq + 10)).collect();
    head.inbox = (1..=6).map(inbox_entry).collect();
    seal_index(&mut head, bounds(4)).expect("seals");
    let sealed = head.clone();
    assert_eq!(
        (
            sealed.index.events.pages[0].min_accept_seq,
            sealed.index.events.pages[0].max_accept_seq
        ),
        (11, 14)
    );

    let mut inverted = sealed.clone();
    inverted.index.events.pages[0].min_accept_seq = 15;
    assert_eq!(
        Head::decode(&inverted.encode()),
        Err(CodecError::Invalid {
            field: "event_pages"
        })
    );
    // min == max is a range of one operation, and decodes.
    let mut single = sealed.clone();
    single.index.events.pages[0].min_accept_seq = 14;
    assert!(Head::decode(&single.encode()).is_ok());

    let mut misplaced = sealed.clone();
    misplaced.index.inbox.pages[0].max_accept_seq = 1;
    assert_eq!(
        Head::decode(&misplaced.encode()),
        Err(CodecError::Invalid {
            field: "inbox_pages"
        })
    );

    // The page's bytes are the ones HEAD names, but its events span 11..=14,
    // not the 12..=14 this HEAD records.
    let page = sealed
        .index
        .written
        .iter()
        .find(|w| w.list == ListKind::Events)
        .expect("an events page")
        .clone();
    let mut narrowed = Head::decode(&sealed.encode()).expect("decodes");
    narrowed.index.events.pages[0].min_accept_seq = 12;
    assert_eq!(
        attach_page(&mut narrowed, ListKind::Events, 0, &page.bytes),
        Err(CodecError::Invalid {
            field: "page.accept_seq"
        })
    );
    let mut shallow = Head::decode(&sealed.encode()).expect("decodes");
    attach_page(&mut shallow, ListKind::Events, 0, &page.bytes).expect("attaches");
}

fn object_ref() -> impl Strategy<Value = ObjectRef> {
    (any::<[u8; 32]>(), any::<u64>()).prop_map(|(h, size_bytes)| ObjectRef {
        hash: ObjectHash(h),
        size_bytes,
    })
}

/// A page reference of `list`: an events page carries an `accept_seq` range
/// with `min <= max`, every other page none.
fn page_meta(list: ListKind, first_key: u64) -> impl Strategy<Value = PageMeta> {
    (
        object_ref(),
        any::<u32>(),
        any::<u32>(),
        any::<(u64, u64)>(),
        any::<u64>(),
        vec(any::<u32>(), 0..6),
    )
        .prop_map(
            move |(page, entries, marked, (a, b), referenced_bytes, mut fps)| {
                fps.sort_unstable();
                let (min_accept_seq, max_accept_seq) = if list == ListKind::Events {
                    (a.min(b), a.max(b))
                } else {
                    (0, 0)
                };
                PageMeta {
                    first_key,
                    page,
                    entries,
                    marked,
                    min_accept_seq,
                    max_accept_seq,
                    referenced_bytes,
                    fingerprints: fps,
                }
            },
        )
}

/// A paged directory of `list`: `first_key`s strictly ascending, `tail_from`
/// above the last, or no page at all.
fn directory(list: ListKind) -> impl Strategy<Value = Directory> {
    (any::<u32>(), vec(1u64..1_000, 0..4), 1u64..1_000).prop_flat_map(move |(start, gaps, tail)| {
        let mut keys = Vec::with_capacity(gaps.len());
        let mut key = u64::from(start);
        for gap in gaps {
            keys.push(key);
            key += gap;
        }
        let tail_from = if keys.is_empty() { 0 } else { key + tail };
        keys.into_iter()
            .map(|key| page_meta(list, key))
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
    (
        head(),
        [
            directory(ListKind::Inbox),
            directory(ListKind::Receipts),
            directory(ListKind::Results),
            directory(ListKind::Events),
            directory(ListKind::Timers),
        ],
    )
        .prop_map(|(mut h, [inbox, receipts, results, events, timers])| {
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
        })
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
