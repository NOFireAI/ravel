# Design #4561: fence commit records that land in an hour after maintenance sealed it

Status: design and decision, no code. Written against `main` at `9961091`.
Every claim about the code or the documents carries a `file:line` on that
checkout. A claim about S3 itself that this checkout cannot show is marked
**[external]** and listed again in section 7.

Terms used throughout:

- **Hour `H`, `end(H)`**: an ingest-hour bucket and its exclusive end
  (`crates/ravel-maintain/src/bucket.rs:33-40`).
- **Maintain seal**: `now >= end(H) + max_flush_lifetime +
  clock_skew_allowance` (`crates/ravel-maintain/src/bucket.rs:47-52`,
  `crates/ravel-maintain/src/config.rs:2778-2781`), 65 minutes at defaults
  (`crates/ravel-maintain/src/config.rs:1090-1094`).
- **Fold seal**: the maintain seal plus `fold_safety_margin`
  (`crates/ravel-catalog/src/fold.rs:623-630`), 80 minutes at defaults
  (`crates/ravel-catalog/src/config.rs:10`, `:328`, `:332`).
- **Late L0**: a commit record (and its data object) that becomes visible in
  `H`'s bucket after a maintenance pass has listed `H` as sealed.
- **Partial cover**: a version 1 compaction record that shares inputs with a
  live erasure rewrite that does not hold all of them
  (`crates/ravel-catalog/src/catalog.rs:6055-6057`, `:6142-6146`).

## 0. Decision in five lines

<!-- section 0 -->

## 1. The durability premise

<!-- section 1 -->

## 2. What a late L0 does today

<!-- section 2 -->

## 3. The fence options

<!-- section 3 -->

## 4. Decision

<!-- section 4 -->

## 5. What the decision does not close, and the follow-ups

<!-- section 5 -->

## 6. Claims in the ADRs, docs and properties that this reading shows false

<!-- section 6 -->

## 7. What this document could not verify

<!-- section 7 -->
