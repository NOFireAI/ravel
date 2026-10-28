//! Bounded immutable index pages for the inbox, receipts, results, events and
//! timers (ADR-3331 decision 3, the index-page amendment).
//!
//! Each list is ordered by a key: inbox, receipts and results by
//! `accept_seq`, events by `event_seq`, timers by `generation`. A version 2
//! HEAD holds, per list, a [`Directory`] of sealed pages plus the list's open
//! page inline: the entries whose key is at or above `tail_from`. A sealed
//! page is an `o/<hash>` object written `CreateIfAbsent` before the HEAD CAS
//! that first references it, and is never rewritten; a change to a page's
//! entries writes a new page under a new hash.
//!
//! The kernel stays pure. The store attaches the pages a command needs to the
//! HEAD it read ([`attach_page`]) before it runs the kernel, and every kernel
//! read of a list first asks the directory whether the entries it is about to
//! read are in memory ([`Directory::require`]), so a missing page is a typed
//! refusal, never a wrong answer. On publish [`seal_index`] rebuilds every
//! directory from its list: an unloaded page is kept as it is, a loaded page
//! whose entries did not change keeps its hash, and the open page seals
//! through [`PageWriter`], whose bound check refuses the entry past
//! `Limits::max_page_entries`.

use prost::Message as _;
use ravel_proto::agents::v1 as pb;

use crate::head::{
    CodecError, EventEntry, Head, InboxEntry, ObjectRef, Receipt, ResultEntry, ResultStatus,
    TimerEntry, TimerStatus, ref_from_proto, ref_to_proto,
};
use crate::identity::{Id128, ObjectHash};
use crate::objects;

/// The five paged lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ListKind {
    Inbox,
    Receipts,
    Results,
    Events,
    Timers,
}

impl ListKind {
    pub const ALL: [ListKind; 5] = [
        ListKind::Inbox,
        ListKind::Receipts,
        ListKind::Results,
        ListKind::Events,
        ListKind::Timers,
    ];

    /// Whether this list's pages carry lookup fingerprints.
    pub fn fingerprinted(self) -> bool {
        !matches!(self, ListKind::Events)
    }
}

/// How a list is held. A version 1 HEAD decodes every list `Inline`; the next
/// publish rewrites it `Paged` ([`seal_index`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Representation {
    Inline,
    #[default]
    Paged,
}

/// One sealed page as HEAD records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageMeta {
    pub first_key: u64,
    pub page: ObjectRef,
    pub entries: u32,
    /// Scheduled timers in a timer page, cancelled results in a result page;
    /// zero for the other lists.
    pub marked: u32,
    /// Earliest due time of a scheduled timer in the page; zero otherwise.
    pub min_due_unix_ns: i64,
    /// Bytes of the objects the page's entries reference.
    pub referenced_bytes: u64,
    /// Sorted fingerprints of the entries' lookup identities
    /// ([`PageEntry::fingerprint`]).
    pub fingerprints: Vec<u32>,
}

/// A page object a publish must write before its CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageObject {
    pub list: ListKind,
    pub page: ObjectRef,
    pub bytes: Vec<u8>,
}

/// The sealed pages of one list. `loaded[i]` says whether page `i`'s entries
/// are in the in-memory list; it is not encoded and not compared.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    pub representation: Representation,
    pub pages: Vec<PageMeta>,
    pub tail_from: u64,
    pub loaded: Vec<bool>,
}

impl PartialEq for Directory {
    fn eq(&self, other: &Directory) -> bool {
        self.representation == other.representation
            && self.pages == other.pages
            && self.tail_from == other.tail_from
    }
}

impl Eq for Directory {}

/// The directories of the five lists, and the page objects the last publish
/// built (`written`, not encoded and not compared).
#[derive(Debug, Clone, Default)]
pub struct Index {
    pub inbox: Directory,
    pub receipts: Directory,
    pub results: Directory,
    pub events: Directory,
    pub timers: Directory,
    pub written: Vec<PageObject>,
}

impl PartialEq for Index {
    fn eq(&self, other: &Index) -> bool {
        ListKind::ALL
            .into_iter()
            .all(|l| self.directory(l) == other.directory(l))
    }
}

impl Eq for Index {}

impl Index {
    /// The index of a version 1 HEAD: every list inline.
    pub fn inline() -> Index {
        let dir = Directory {
            representation: Representation::Inline,
            ..Directory::default()
        };
        Index {
            inbox: dir.clone(),
            receipts: dir.clone(),
            results: dir.clone(),
            events: dir.clone(),
            timers: dir,
            written: Vec::new(),
        }
    }

    pub fn directory(&self, list: ListKind) -> &Directory {
        match list {
            ListKind::Inbox => &self.inbox,
            ListKind::Receipts => &self.receipts,
            ListKind::Results => &self.results,
            ListKind::Events => &self.events,
            ListKind::Timers => &self.timers,
        }
    }

    pub fn directory_mut(&mut self, list: ListKind) -> &mut Directory {
        match list {
            ListKind::Inbox => &mut self.inbox,
            ListKind::Receipts => &mut self.receipts,
            ListKind::Results => &mut self.results,
            ListKind::Events => &mut self.events,
            ListKind::Timers => &mut self.timers,
        }
    }

    /// Every sealed page this root references.
    pub fn page_refs(&self) -> impl Iterator<Item = ObjectRef> + '_ {
        ListKind::ALL
            .into_iter()
            .flat_map(|l| self.directory(l).pages.iter().map(|p| p.page))
    }

    /// Every sealed page not in memory, by list.
    pub fn unloaded(&self) -> Vec<(ListKind, usize)> {
        ListKind::ALL
            .into_iter()
            .flat_map(|l| {
                self.directory(l)
                    .unloaded()
                    .into_iter()
                    .map(move |i| (l, i))
            })
            .collect()
    }

    /// Whether any list is still held inline (a version 1 HEAD).
    pub fn any_inline(&self) -> bool {
        ListKind::ALL
            .into_iter()
            .any(|l| self.directory(l).representation == Representation::Inline)
    }
}

/// A page refusal. `NotLoaded` is a caller that ran the kernel without a page
/// the command reads; `PageFull` and `DirectoryFull` are the two bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PageError {
    #[error("{list:?} index page {page} is not loaded")]
    NotLoaded { list: ListKind, page: usize },
    #[error("index page holds its maximum of {max} entries")]
    PageFull { max: usize },
    #[error("{list:?} index directory is past its maximum of {max} pages")]
    DirectoryFull { list: ListKind, max: usize },
}

/// The bounds [`seal_index`] enforces, from [`crate::kernel::Limits`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageBounds {
    /// `Limits::max_page_entries`.
    pub max_page_entries: usize,
    /// `Limits::max_index_pages`.
    pub max_index_pages: usize,
}

/// The entries a kernel read touches, as the directory can locate them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// The entry with this key.
    Key(u64),
    /// The entry with the lowest key.
    First,
    /// Every entry whose lookup identity has this fingerprint.
    Fingerprint(u32),
    /// Every marked entry (scheduled timer, cancelled result).
    Marked,
    /// Every entry.
    All,
}

/// One list's entry type: its key, its lookup fingerprint and its page codec.
pub trait PageEntry: Clone + PartialEq + Sized {
    const LIST: ListKind;
    /// The open page never stays empty while a sealed page exists, so a
    /// HEAD read without its pages still answers "is the list empty".
    const KEEP_TAIL: bool = false;
    fn key(&self) -> u64;
    fn fingerprint(&self) -> Option<u32> {
        None
    }
    fn marked(&self) -> bool {
        false
    }
    fn due(&self) -> Option<i64> {
        None
    }
    fn referenced_bytes(&self) -> u64;
    fn encode_page(entries: &[Self]) -> Vec<u8>;
    fn decode_page(bytes: &[u8]) -> Result<Vec<Self>, CodecError>;
    fn list(head: &Head) -> &Vec<Self>;
    fn list_mut(head: &mut Head) -> &mut Vec<Self>;
}

/// First four bytes of a domain-separated blake3 digest.
fn fingerprint(domain: &[u8], parts: &[&[u8]]) -> u32 {
    let mut h = blake3::Hasher::new();
    h.update(domain);
    for part in parts {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    let digest = h.finalize();
    let b = digest.as_bytes();
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// The fingerprint a receipt lookup by `(handler, request_id)` searches.
pub fn receipt_fingerprint(handler: &str, request_id: &str) -> u32 {
    fingerprint(
        b"ravel-agent-v1/page/receipt",
        &[handler.as_bytes(), request_id.as_bytes()],
    )
}

/// The fingerprint a lookup by a turn id or a timer id searches.
pub fn id_fingerprint(id: &Id128) -> u32 {
    fingerprint(b"ravel-agent-v1/page/id", &[&id.0])
}

fn ref_bytes(r: &Option<ObjectRef>) -> u64 {
    r.map_or(0, |r| r.size_bytes)
}

impl PageEntry for InboxEntry {
    const LIST: ListKind = ListKind::Inbox;
    const KEEP_TAIL: bool = true;
    fn key(&self) -> u64 {
        self.accept_seq
    }
    fn fingerprint(&self) -> Option<u32> {
        self.last_failed_turn_id.as_ref().map(id_fingerprint)
    }
    fn referenced_bytes(&self) -> u64 {
        ref_bytes(&self.payload)
    }
    fn encode_page(entries: &[Self]) -> Vec<u8> {
        objects::encode_inbox_page(entries)
    }
    fn decode_page(bytes: &[u8]) -> Result<Vec<Self>, CodecError> {
        objects::decode_inbox_page(bytes)
    }
    fn list(head: &Head) -> &Vec<Self> {
        &head.inbox
    }
    fn list_mut(head: &mut Head) -> &mut Vec<Self> {
        &mut head.inbox
    }
}

impl PageEntry for Receipt {
    const LIST: ListKind = ListKind::Receipts;
    fn key(&self) -> u64 {
        self.accept_seq
    }
    fn fingerprint(&self) -> Option<u32> {
        Some(receipt_fingerprint(&self.handler, &self.request_id))
    }
    fn referenced_bytes(&self) -> u64 {
        0
    }
    fn encode_page(entries: &[Self]) -> Vec<u8> {
        objects::encode_receipt_page(entries)
    }
    fn decode_page(bytes: &[u8]) -> Result<Vec<Self>, CodecError> {
        objects::decode_receipt_page(bytes)
    }
    fn list(head: &Head) -> &Vec<Self> {
        &head.receipts
    }
    fn list_mut(head: &mut Head) -> &mut Vec<Self> {
        &mut head.receipts
    }
}

impl PageEntry for ResultEntry {
    const LIST: ListKind = ListKind::Results;
    fn key(&self) -> u64 {
        self.accept_seq
    }
    fn fingerprint(&self) -> Option<u32> {
        self.turn_id.as_ref().map(id_fingerprint)
    }
    fn marked(&self) -> bool {
        self.status == ResultStatus::Cancelled
    }
    fn referenced_bytes(&self) -> u64 {
        ref_bytes(&self.output)
    }
    fn encode_page(entries: &[Self]) -> Vec<u8> {
        objects::encode_result_page(entries)
    }
    fn decode_page(bytes: &[u8]) -> Result<Vec<Self>, CodecError> {
        objects::decode_result_page(bytes)
    }
    fn list(head: &Head) -> &Vec<Self> {
        &head.results
    }
    fn list_mut(head: &mut Head) -> &mut Vec<Self> {
        &mut head.results
    }
}

impl PageEntry for EventEntry {
    const LIST: ListKind = ListKind::Events;
    fn key(&self) -> u64 {
        self.event_seq
    }
    fn referenced_bytes(&self) -> u64 {
        ref_bytes(&self.payload)
    }
    fn encode_page(entries: &[Self]) -> Vec<u8> {
        objects::encode_event_page(entries)
    }
    fn decode_page(bytes: &[u8]) -> Result<Vec<Self>, CodecError> {
        objects::decode_event_page(bytes)
    }
    fn list(head: &Head) -> &Vec<Self> {
        &head.events
    }
    fn list_mut(head: &mut Head) -> &mut Vec<Self> {
        &mut head.events
    }
}

impl PageEntry for TimerEntry {
    const LIST: ListKind = ListKind::Timers;
    fn key(&self) -> u64 {
        self.generation
    }
    fn fingerprint(&self) -> Option<u32> {
        Some(id_fingerprint(&self.timer_id))
    }
    fn marked(&self) -> bool {
        self.status == TimerStatus::Scheduled
    }
    fn due(&self) -> Option<i64> {
        self.marked().then_some(self.due_unix_ns)
    }
    fn referenced_bytes(&self) -> u64 {
        ref_bytes(&self.payload)
    }
    fn encode_page(entries: &[Self]) -> Vec<u8> {
        objects::encode_timer_table(entries)
    }
    fn decode_page(bytes: &[u8]) -> Result<Vec<Self>, CodecError> {
        objects::decode_timer_table(bytes)
    }
    fn list(head: &Head) -> &Vec<Self> {
        &head.timers
    }
    fn list_mut(head: &mut Head) -> &mut Vec<Self> {
        &mut head.timers
    }
}

/// The page being filled. [`PageWriter::push`] is the only way an entry
/// enters a sealed page, and it refuses the entry past
/// `Limits::max_page_entries`.
#[derive(Debug)]
pub struct PageWriter<T> {
    entries: Vec<T>,
    max: usize,
}

impl<T: PageEntry> PageWriter<T> {
    pub fn new(max_page_entries: usize) -> Self {
        PageWriter {
            entries: Vec::new(),
            max: max_page_entries.max(1),
        }
    }

    pub fn push(&mut self, entry: T) -> Result<(), PageError> {
        if self.entries.len() >= self.max {
            return Err(PageError::PageFull { max: self.max });
        }
        self.entries.push(entry);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Encode the page and describe it for HEAD.
    pub fn finish(self, first_key: u64) -> (PageMeta, Vec<u8>) {
        let bytes = T::encode_page(&self.entries);
        let mut fingerprints: Vec<u32> = self.entries.iter().filter_map(T::fingerprint).collect();
        fingerprints.sort_unstable();
        let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        let meta = PageMeta {
            first_key,
            page: ObjectRef::of(&bytes),
            entries: count(self.entries.len()),
            marked: count(self.entries.iter().filter(|e| e.marked()).count()),
            min_due_unix_ns: self.entries.iter().filter_map(T::due).min().unwrap_or(0),
            referenced_bytes: self
                .entries
                .iter()
                .fold(0u64, |acc, e| acc.saturating_add(e.referenced_bytes())),
            fingerprints,
        };
        (meta, bytes)
    }
}

impl Directory {
    /// The page whose range holds `key`, or `None` when the key is in the
    /// open page. Page `i` holds `[first_key_i, first_key_{i+1})`, the last
    /// page runs up to `tail_from`, and the first also holds every key below
    /// its own `first_key`.
    pub fn page_for(&self, key: u64) -> Option<usize> {
        if self.pages.is_empty() || key >= self.tail_from {
            return None;
        }
        let after = self.pages.partition_point(|p| p.first_key <= key);
        Some(after.saturating_sub(1))
    }

    pub fn is_loaded(&self, page: usize) -> bool {
        self.loaded.get(page).copied().unwrap_or(false)
    }

    /// Every page not in memory.
    pub fn unloaded(&self) -> Vec<usize> {
        (0..self.pages.len())
            .filter(|&i| !self.is_loaded(i))
            .collect()
    }

    /// The pages that may hold an entry `need` names. A fingerprint selects
    /// only the pages whose sorted fingerprint set lists it.
    pub fn pages_for(&self, need: Need) -> Vec<usize> {
        let all = 0..self.pages.len();
        match need {
            Need::Key(key) => self.page_for(key).into_iter().collect(),
            Need::First => all.take(1).collect(),
            Need::Fingerprint(fp) => all
                .filter(|&i| self.pages[i].fingerprints.binary_search(&fp).is_ok())
                .collect(),
            Need::Marked => all.filter(|&i| self.pages[i].marked > 0).collect(),
            Need::All => all.collect(),
        }
    }

    /// The pages `need` names that are not in memory.
    pub fn missing(&self, need: Need) -> Vec<usize> {
        self.pages_for(need)
            .into_iter()
            .filter(|&i| !self.is_loaded(i))
            .collect()
    }

    /// Every entry `need` names is in memory.
    pub fn require(&self, list: ListKind, need: Need) -> Result<(), PageError> {
        match self.missing(need).first() {
            Some(&page) => Err(PageError::NotLoaded { list, page }),
            None => Ok(()),
        }
    }

    /// The pages whose key ranges meet `[lo, hi]`.
    pub fn pages_in_range(&self, lo: u64, hi: u64) -> Vec<usize> {
        if self.pages.is_empty() || lo > hi || lo >= self.tail_from {
            return Vec::new();
        }
        let first = self.page_for(lo).unwrap_or(0);
        let last = self
            .page_for(hi)
            .unwrap_or(self.pages.len().saturating_sub(1));
        (first..=last).collect()
    }

    /// Entries the list holds: those of the pages not in memory plus the
    /// `in_memory` ones.
    pub fn total(&self, in_memory: usize) -> u64 {
        self.unloaded().into_iter().fold(in_memory as u64, |acc, i| {
            acc.saturating_add(u64::from(self.pages[i].entries))
        })
    }

    /// Marked entries in the pages not in memory.
    pub fn unloaded_marked(&self) -> u64 {
        self.unloaded().into_iter().fold(0u64, |acc, i| {
            acc.saturating_add(u64::from(self.pages[i].marked))
        })
    }

    /// Referenced bytes of the pages not in memory.
    pub fn unloaded_referenced_bytes(&self) -> u64 {
        self.unloaded()
            .into_iter()
            .fold(0u64, |acc, i| acc.saturating_add(self.pages[i].referenced_bytes))
    }

    /// Whether the entry with `key` is encoded inline in HEAD.
    pub fn is_inline(&self, key: u64) -> bool {
        self.page_for(key).is_none()
    }

    fn mark_loaded(&mut self, page: usize) {
        if self.loaded.len() < self.pages.len() {
            self.loaded.resize(self.pages.len(), false);
        }
        if let Some(slot) = self.loaded.get_mut(page) {
            *slot = true;
        }
    }
}

fn invalid(field: &'static str) -> CodecError {
    CodecError::Invalid { field }
}

fn attach<T: PageEntry>(head: &mut Head, page: usize, bytes: &[u8]) -> Result<(), CodecError> {
    let dir = head.index.directory(T::LIST);
    let meta = dir.pages.get(page).ok_or(invalid("page.index"))?;
    if ObjectRef::of(bytes) != meta.page {
        return Err(invalid("page.hash"));
    }
    if dir.is_loaded(page) {
        return Ok(());
    }
    let entries = T::decode_page(bytes)?;
    if entries.len() != meta.entries as usize {
        return Err(invalid("page.entries"));
    }
    if entries.iter().any(|e| dir.page_for(e.key()) != Some(page)) {
        return Err(invalid("page.first_key"));
    }
    head.index.directory_mut(T::LIST).mark_loaded(page);
    let list = T::list_mut(head);
    list.extend(entries);
    list.sort_by_key(T::key);
    Ok(())
}

/// Put the entries of sealed page `page` of `list`, read from `o/<hash>`, into
/// `head`'s in-memory list. Refuses bytes whose hash, entry count or key range
/// is not the one HEAD records; a page already attached is a no-op.
pub fn attach_page(
    head: &mut Head,
    list: ListKind,
    page: usize,
    bytes: &[u8],
) -> Result<(), CodecError> {
    match list {
        ListKind::Inbox => attach::<InboxEntry>(head, page, bytes),
        ListKind::Receipts => attach::<Receipt>(head, page, bytes),
        ListKind::Results => attach::<ResultEntry>(head, page, bytes),
        ListKind::Events => attach::<EventEntry>(head, page, bytes),
        ListKind::Timers => attach::<TimerEntry>(head, page, bytes),
    }
}

/// Rebuild `dir` from the in-memory `list` after a transition. An `Inline`
/// (version 1) list moves to `Paged` here, in the same publish. Returns the
/// new directory and the page objects to write before the CAS.
///
/// The open page seals only entries below `seal_below`, the lowest key a
/// later append to this list may still carry, so an entry is never appended
/// into a sealed page's range.
pub fn seal<T: PageEntry>(
    dir: &Directory,
    list: &[T],
    seal_below: u64,
    bounds: PageBounds,
) -> Result<(Directory, Vec<PageObject>), PageError> {
    let mut source = match dir.representation {
        Representation::Inline => Directory::default(),
        Representation::Paged => dir.clone(),
    };
    let k = bounds.max_page_entries.max(1);
    let mut objects = Vec::new();
    let mut by_page: Vec<Vec<T>> = vec![Vec::new(); source.pages.len()];
    let mut open: Vec<T> = Vec::new();
    for entry in list {
        match source.page_for(entry.key()) {
            Some(i) => by_page[i].push(entry.clone()),
            None => open.push(entry.clone()),
        }
    }
    let last = source.pages.len().checked_sub(1);
    if let Some(last) = last.filter(|&i| T::KEEP_TAIL && open.is_empty() && source.is_loaded(i)) {
        open = by_page.pop().unwrap_or_default();
        let meta = source.pages.remove(last);
        source.loaded.truncate(last);
        source.tail_from = if last == 0 { 0 } else { meta.first_key };
    }
    let mut out = Directory {
        representation: Representation::Paged,
        pages: Vec::new(),
        tail_from: source.tail_from,
        loaded: Vec::new(),
    };
    for (i, mut entries) in by_page.into_iter().enumerate() {
        let meta = &source.pages[i];
        if !source.is_loaded(i) {
            if !entries.is_empty() {
                return Err(PageError::NotLoaded {
                    list: T::LIST,
                    page: i,
                });
            }
            out.pages.push(meta.clone());
            out.loaded.push(false);
            continue;
        }
        entries.sort_by_key(T::key);
        let mut first_key = meta.first_key;
        for chunk in entries.chunks(k) {
            let mut writer = PageWriter::new(k);
            for e in chunk {
                writer.push(e.clone())?;
            }
            let (new_meta, bytes) = writer.finish(first_key);
            if new_meta != *meta {
                objects.push(PageObject {
                    list: T::LIST,
                    page: new_meta.page,
                    bytes,
                });
            }
            out.pages.push(new_meta);
            out.loaded.push(true);
            first_key = chunk
                .last()
                .map_or(first_key, |e| e.key().saturating_add(1));
        }
    }
    open.sort_by_key(T::key);
    let mut start = 0;
    while open.len() - start > k && open[start + k].key() <= seal_below {
        let mut writer = PageWriter::new(k);
        for e in &open[start..start + k] {
            writer.push(e.clone())?;
        }
        let (meta, bytes) = writer.finish(open[start].key());
        objects.push(PageObject {
            list: T::LIST,
            page: meta.page,
            bytes,
        });
        out.pages.push(meta);
        out.loaded.push(true);
        out.tail_from = open[start + k].key();
        start += k;
    }
    if out.pages.is_empty() {
        out.tail_from = 0;
    }
    if out.pages.len() > bounds.max_index_pages {
        return Err(PageError::DirectoryFull {
            list: T::LIST,
            max: bounds.max_index_pages,
        });
    }
    Ok((out, objects))
}

fn seal_list<T: PageEntry>(
    head: &mut Head,
    seal_below: u64,
    bounds: PageBounds,
    written: &mut Vec<PageObject>,
) -> Result<(), PageError> {
    let (dir, objects) = seal(
        head.index.directory(T::LIST),
        T::list(head),
        seal_below,
        bounds,
    )?;
    *head.index.directory_mut(T::LIST) = dir;
    written.extend(objects);
    Ok(())
}

/// [`seal`] every list of `head` and record the page objects to write in
/// `head.index.written`. Results seal only below the lowest `accept_seq`
/// still pending (in the inbox or the active turn), since a pending input's
/// result is appended later under that key; a sealed first inbox page
/// answers with its `first_key`, which is at or below every key it holds.
/// Timers seal only below the lowest scheduled generation, so a sealed timer
/// page never holds a scheduled timer and the scheduled set is always in
/// HEAD itself (a rescheduled timer takes a new, highest generation).
pub fn seal_index(head: &mut Head, bounds: PageBounds) -> Result<(), PageError> {
    let pending_floor = head
        .active_turn
        .iter()
        .map(|t| t.input_accept_seq)
        .chain(head.index.inbox.pages.first().map(|p| p.first_key))
        .chain(head.inbox.iter().map(|e| e.accept_seq))
        .min()
        .unwrap_or(head.next_accept_seq);
    let mut written = Vec::new();
    seal_list::<InboxEntry>(head, u64::MAX, bounds, &mut written)?;
    seal_list::<Receipt>(head, u64::MAX, bounds, &mut written)?;
    seal_list::<ResultEntry>(head, pending_floor, bounds, &mut written)?;
    seal_list::<EventEntry>(head, u64::MAX, bounds, &mut written)?;
    let first_scheduled = head
        .timers
        .iter()
        .filter(|t| t.marked())
        .map(|t| t.generation)
        .min()
        .unwrap_or(u64::MAX);
    seal_list::<TimerEntry>(head, first_scheduled, bounds, &mut written)?;
    head.index.written = written;
    Ok(())
}

impl Head {
    /// Inputs waiting in the inbox, sealed pages included.
    pub fn inbox_depth(&self) -> u64 {
        self.index.inbox.total(self.inbox.len())
    }

    /// Receipts held, sealed pages included.
    pub fn receipt_count(&self) -> u64 {
        self.index.receipts.total(self.receipts.len())
    }

    /// Scheduled timers, sealed pages included.
    pub fn scheduled_timers(&self) -> u64 {
        let in_memory = self
            .timers
            .iter()
            .filter(|t| t.status == TimerStatus::Scheduled)
            .count() as u64;
        self.index.timers.unloaded_marked().saturating_add(in_memory)
    }

    /// Whether the inbox holds an input, sealed pages included.
    pub fn has_inbox(&self) -> bool {
        self.inbox_depth() > 0
    }
}

/// A version 2 list with no sealed page encodes no directory, so a small HEAD
/// pays nothing for the paged representation.
pub(crate) fn directory_to_proto(dir: &Directory) -> Option<pb::PageDirectory> {
    if dir.pages.is_empty() {
        return None;
    }
    Some(page_directory(dir))
}

fn page_directory(dir: &Directory) -> pb::PageDirectory {
    pb::PageDirectory {
        representation: pb::IndexRepresentation::Paged as i32,
        pages: dir
            .pages
            .iter()
            .map(|p| pb::PageRef {
                first_key: p.first_key,
                page: Some(ref_to_proto(p.page)),
                entries: p.entries,
                marked: p.marked,
                min_due_unix_ns: p.min_due_unix_ns,
                referenced_bytes: p.referenced_bytes,
                fingerprints: p.fingerprints.iter().flat_map(|f| f.to_be_bytes()).collect(),
            })
            .collect(),
        tail_from: dir.tail_from,
    }
}

/// Decode and validate one version 2 directory: `PAGED`, `first_key`s
/// strictly ascending and below `tail_from`, every page reference present,
/// fingerprints whole 4-byte values in ascending order. An absent directory
/// is a paged list with no sealed page; a present one holds at least one.
pub(crate) fn directory_from_proto(
    dir: Option<pb::PageDirectory>,
    field: &'static str,
) -> Result<Directory, CodecError> {
    let Some(dir) = dir else {
        return Ok(Directory::default());
    };
    if dir.pages.is_empty() {
        return Err(invalid(field));
    }
    if dir.representation != pb::IndexRepresentation::Paged as i32 {
        return Err(CodecError::UnsupportedRepresentation {
            found: dir.representation,
        });
    }
    let mut pages = Vec::with_capacity(dir.pages.len());
    for p in dir.pages {
        if p.fingerprints.len() % 4 != 0 {
            return Err(invalid(field));
        }
        let fingerprints: Vec<u32> = p
            .fingerprints
            .chunks_exact(4)
            .map(|c| u32::from_be_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        if fingerprints.windows(2).any(|w| w[0] > w[1]) {
            return Err(invalid(field));
        }
        let page = ref_from_proto(p.page, field)?.ok_or(invalid(field))?;
        pages.push(PageMeta {
            first_key: p.first_key,
            page,
            entries: p.entries,
            marked: p.marked,
            min_due_unix_ns: p.min_due_unix_ns,
            referenced_bytes: p.referenced_bytes,
            fingerprints,
        });
    }
    if pages.windows(2).any(|w| w[0].first_key >= w[1].first_key)
        || pages.last().is_some_and(|p| p.first_key >= dir.tail_from)
        || (pages.is_empty() && dir.tail_from != 0)
    {
        return Err(invalid(field));
    }
    Ok(Directory {
        representation: Representation::Paged,
        loaded: vec![false; pages.len()],
        pages,
        tail_from: dir.tail_from,
    })
}

/// Encoded bytes one page reference at its widest adds to a directory of
/// `list`, fingerprints for `max_page_entries` entries included: the bytes
/// the growth guard reserves per page a later exempt publish may seal.
pub fn max_page_ref_bytes(list: ListKind, max_page_entries: usize) -> usize {
    let widest = PageMeta {
        first_key: u64::MAX - 1,
        page: ObjectRef {
            hash: ObjectHash([u8::MAX; 32]),
            size_bytes: u64::MAX,
        },
        entries: u32::MAX,
        marked: u32::MAX,
        min_due_unix_ns: i64::MIN,
        referenced_bytes: u64::MAX,
        fingerprints: if list.fingerprinted() {
            vec![u32::MAX; max_page_entries]
        } else {
            Vec::new()
        },
    };
    let one = Directory {
        pages: vec![widest],
        tail_from: u64::MAX,
        ..Directory::default()
    };
    // A whole one-page directory in its AgentHead field, tag and length prefix
    // included, so the first page of a list (which also adds the directory)
    // is covered as well as every later one.
    let len = page_directory(&one).encoded_len();
    len + prost::encoding::encoded_len_varint(len as u64) + 2
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests;
