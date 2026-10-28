//! The publication skeleton of ADR-3331 decision 4. Every durable command
//! the crate runs goes through [`AgentStore::publish`]:
//!
//! 1. GET HEAD and keep `GetOutcome.version` as the CAS token
//!    ([`AgentStore::read_head_state`]).
//! 2. Resolve the command's operation id against the receipt index, and
//! 3. validate its preconditions: both are the kernel's
//!    ([`ravel_agents_core::kernel::Kernel::apply`]), run on the root just
//!    read. A duplicate comes back `Unchanged` and nothing is written
//!    (`tests::enqueue_with_the_same_request_id_and_hash_returns_the_same_handle`).
//! 4. Build the dependencies: the objects the new root references that the
//!    old one did not ([`new_dependencies`]).
//! 5. PUT each with `CreateIfAbsent`; on `AlreadyExists` read it back and
//!    compare the content hash ([`AgentStore::store_dependency`]).
//! 6. CAS HEAD from the version read in step 1 to the new root, which the
//!    kernel built with `root_revision + 1`.
//! 7. Return success only when that write returned `Ok`.
//!
//! `PreconditionFailed` on the CAS (and `AlreadyExists` on the create of a
//! root that was absent in step 1) re-runs the whole skeleton from step 1,
//! at most [`MAX_CAS_ATTEMPTS`] times, so the kernel revalidates the command
//! against the root another writer published
//! (`tests::a_failed_conditional_write_re_reads_and_succeeds_on_retry`).
//!
//! `Transient`, `Timeout` or `Throttled` from the HEAD write is an unknown
//! outcome: the write may have applied. The command returns `OutcomeUnknown`
//! carrying the operation handle the kernel assigned, never a success and
//! never a retry under a fresh id; the caller resolves that handle through
//! [`AgentStore::resolve`], which reads the receipt index
//! (`tests::cas_applied_with_lost_response_is_resolved_by_operation_id_after_a_later_revision`).

use bytes::Bytes;
use ravel_agents_core::command::Command;
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::{CodecError, Head, MAX_HEAD_BYTES, ObjectRef};
use ravel_agents_core::identity::{AgentId, agent_id};
use ravel_agents_core::kernel::{Effect, Reply, Transition};
use ravel_agents_core::keys::{head_key, object_key};
use ravel_agents_core::state::Handle;
use ravel_object_store::{GetRange, PutMode, PutOptions, StoreError, UploadChecksum, Version};

use crate::counters::{Phase, Request};
use crate::wake;
use crate::{AgentStore, HeadState, Outcome};

/// Most times one command runs the skeleton from step 1 after a refused
/// conditional HEAD write before it gives up with `StoreUnavailable`.
pub const MAX_CAS_ATTEMPTS: u32 = 8;

/// Most attempts at one read, or one dependency write, that failed with a
/// retryable store error (or a `NotFound` the caller does not trust) before
/// it reports `StoreUnavailable`. Dependency writes are content-addressed
/// `CreateIfAbsent`, so repeating one is safe whatever the first attempt did.
pub const MAX_READ_ATTEMPTS: u32 = 3;

pub(crate) fn ids_for(agent: &AgentId) -> SafeIds {
    SafeIds {
        agent_id: Some(agent.to_hex()),
        ..SafeIds::default()
    }
}

fn ids_for_head(agent: &AgentId, head: &Head) -> SafeIds {
    SafeIds {
        incarnation: Some(head.incarnation),
        ..ids_for(agent)
    }
}

pub(crate) fn unavailable(agent: &AgentId, reason: &'static str) -> AgentError {
    AgentError::new(ErrorKind::StoreUnavailable, ids_for(agent), reason)
}

/// A record that failed to decode. Version skew keeps its own kind; there is
/// no corruption kind among the eleven, so anything else is reported as the
/// store not answering usably (`Backoff`).
pub(crate) fn codec_error(agent: &AgentId, error: &CodecError) -> AgentError {
    tracing::warn!(agent = %agent.to_hex(), %error, "agents record failed to decode");
    match error.kind() {
        Some(kind) => AgentError::new(kind, ids_for(agent), "record format is not supported"),
        None => AgentError::new(
            ErrorKind::StoreUnavailable,
            ids_for(agent),
            "stored record failed to decode",
        ),
    }
}

/// The objects `new` references that `old` does not, each once.
pub(crate) fn new_dependencies(old: Option<&Head>, new: &Head) -> Vec<ObjectRef> {
    let existing: Vec<ObjectRef> = old.map(|h| h.object_refs().collect()).unwrap_or_default();
    let mut out: Vec<ObjectRef> = Vec::new();
    for r in new.object_refs() {
        if !existing.contains(&r) && !out.contains(&r) {
            out.push(r);
        }
    }
    out
}

fn checksum(bytes: &[u8]) -> UploadChecksum {
    UploadChecksum::Crc32c(crc32c::crc32c(bytes))
}

/// The handle of the operation a reply names, if it names one.
pub(crate) fn reply_handle(agent: &AgentId, head: &Head, reply: &Reply) -> Option<Handle> {
    match reply {
        Reply::Accepted { accept_seq, .. } => {
            Some(Handle::operation(*agent, head.incarnation, *accept_seq))
        }
        _ => None,
    }
}

impl AgentStore {
    /// Step 1. `confirm_absence` re-reads a `NotFound` up to
    /// [`MAX_READ_ATTEMPTS`] times before believing it; create skips that,
    /// because its `CreateIfAbsent` refuses on a root a blip hid.
    pub(crate) async fn read_head_state(
        &self,
        agent: &AgentId,
        confirm_absence: bool,
    ) -> Result<HeadState, AgentError> {
        let key = head_key(&self.tenant, agent)
            .map_err(|_| unavailable(agent, "agent key is not addressable"))?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let last = attempt >= MAX_READ_ATTEMPTS;
            self.counters.request(Phase::Resolve, Request::Get);
            match self.store.get(&key, GetRange::Full).await {
                Ok(got) => {
                    self.counters.bytes_in(Phase::Resolve, got.data.len());
                    return self.decode_head(agent, &got.data, got.version);
                }
                Err(StoreError::NotFound) if confirm_absence && !last => continue,
                Err(StoreError::NotFound) => return Ok(HeadState::Absent),
                Err(error) if error.is_retryable() && !last => {
                    tracing::debug!(agent = %agent.to_hex(), %error, "agents HEAD read retried");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(agent = %agent.to_hex(), %error, "agents HEAD read failed");
                    return Err(unavailable(agent, "HEAD could not be read"));
                }
            }
        }
    }

    fn decode_head(
        &self,
        agent: &AgentId,
        bytes: &[u8],
        version: Version,
    ) -> Result<HeadState, AgentError> {
        if bytes.len() > MAX_HEAD_BYTES {
            return Err(unavailable(agent, "HEAD is larger than the byte cap"));
        }
        let head = match Head::decode(bytes) {
            Ok(head) => head,
            Err(CodecError::UnsupportedFormat { found }) => {
                return Ok(HeadState::Unsupported { found });
            }
            Err(error) => return Err(codec_error(agent, &error)),
        };
        if head.tenant != self.tenant || agent_id(&head.kind, &head.key) != *agent {
            return Err(unavailable(agent, "HEAD names a different agent"));
        }
        Ok(HeadState::Present {
            head: Box::new(head),
            version,
        })
    }

    /// Steps 1 to 7 for one command. `deps` holds the bytes of objects the
    /// command references, keyed by their content; a referenced object not
    /// among them must already be in the store.
    pub(crate) async fn publish(
        &self,
        agent: &AgentId,
        command: &Command,
        deps: &[Bytes],
    ) -> Result<Outcome, AgentError> {
        if deps.iter().any(|d| d.len() > self.limits.max_object_bytes) {
            return Err(AgentError::new(
                ErrorKind::BudgetExceeded,
                ids_for(agent),
                "object is larger than the per-object byte limit",
            ));
        }
        let key = head_key(&self.tenant, agent)
            .map_err(|_| unavailable(agent, "agent key is not addressable"))?;
        let is_create = matches!(command, Command::Create(_));
        let mut attempt = 0;
        loop {
            attempt += 1;
            let (current, version) = match self.read_head_state(agent, !is_create).await? {
                HeadState::Absent => (None, None),
                HeadState::Present { head, version } => (Some(*head), Some(version)),
                HeadState::Unsupported { .. } => {
                    return Err(AgentError::new(
                        ErrorKind::UnsupportedFormat,
                        ids_for(agent),
                        "HEAD format version is newer than this release",
                    ));
                }
            };
            let now = self.clock.now_unix_ns();
            let transition =
                self.kernel
                    .apply(current.as_ref(), command, now, self.ids.as_ref())?;
            let (new, effects, reply) = match transition {
                Transition::Unchanged { reply } => {
                    let (Some(head), Some(version)) = (current, version) else {
                        return Err(unavailable(agent, "kernel left an absent root unchanged"));
                    };
                    return Ok(Outcome {
                        agent: *agent,
                        reply,
                        head,
                        version,
                        published: false,
                        effects: Vec::new(),
                    });
                }
                Transition::Publish {
                    head,
                    effects,
                    reply,
                } => (head, effects, reply),
            };

            let new_deps = new_dependencies(current.as_ref(), &new);
            if new_deps
                .iter()
                .any(|d| d.size_bytes > self.limits.max_object_bytes as u64)
            {
                return Err(AgentError::new(
                    ErrorKind::BudgetExceeded,
                    ids_for_head(agent, &new),
                    "object is larger than the per-object byte limit",
                ));
            }
            let body = new.encode();
            if body.len() > MAX_HEAD_BYTES {
                return Err(AgentError::new(
                    ErrorKind::BudgetExceeded,
                    ids_for_head(agent, &new),
                    "HEAD would exceed its byte cap",
                ));
            }

            for dep in new_deps {
                self.store_dependency(agent, dep, deps).await?;
            }

            let mode = match &version {
                None => PutMode::CreateIfAbsent,
                Some(observed) => PutMode::CasVersion(observed.clone()),
            };
            let request = match mode {
                PutMode::CasVersion(_) => Request::Cas,
                _ => Request::Put,
            };
            let opts = PutOptions {
                mode,
                checksum: Some(checksum(&body)),
            };
            self.counters.request(Phase::Publish, request);
            self.counters.bytes_out(Phase::Publish, body.len());
            match self.store.put(&key, Bytes::from(body), opts).await {
                Ok(put) => {
                    self.after_publish(agent, &effects).await;
                    if let Reply::Blocked(error) = reply {
                        return Err(error);
                    }
                    return Ok(Outcome {
                        agent: *agent,
                        reply,
                        head: new,
                        version: put.version,
                        published: true,
                        effects,
                    });
                }
                Err(StoreError::PreconditionFailed | StoreError::AlreadyExists) => {
                    self.counters.conflict(Phase::Publish);
                    if attempt >= MAX_CAS_ATTEMPTS {
                        return Err(unavailable(
                            agent,
                            "HEAD stayed contended past the CAS bound",
                        ));
                    }
                }
                Err(error) if error.is_retryable() => {
                    tracing::warn!(agent = %agent.to_hex(), %error, "agents HEAD write outcome unknown");
                    let ids = SafeIds {
                        operation: reply_handle(agent, &new, &reply).map(|h| h.encode()),
                        ..ids_for_head(agent, &new)
                    };
                    return Err(AgentError::new(
                        ErrorKind::OutcomeUnknown,
                        ids,
                        "HEAD write outcome is unknown; resolve the operation",
                    ));
                }
                Err(error) => {
                    tracing::warn!(agent = %agent.to_hex(), %error, "agents HEAD write refused");
                    return Err(unavailable(agent, "HEAD write was refused"));
                }
            }
        }
    }

    /// Step 5 for one object the new root references.
    async fn store_dependency(
        &self,
        agent: &AgentId,
        dep: ObjectRef,
        supplied: &[Bytes],
    ) -> Result<(), AgentError> {
        let key = object_key(&self.tenant, agent, &dep.hash)
            .map_err(|_| unavailable(agent, "object key is not addressable"))?;
        match supplied.iter().find(|b| ObjectRef::of(b) == dep) {
            Some(bytes) => self.put_object_at(agent, &key, dep, bytes.clone()).await,
            None => self.probe_object(agent, &key, dep).await,
        }
    }

    pub(crate) async fn put_object_at(
        &self,
        agent: &AgentId,
        key: &str,
        dep: ObjectRef,
        bytes: Bytes,
    ) -> Result<(), AgentError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let opts = PutOptions::create_if_absent().with_checksum(checksum(&bytes));
            self.counters.request(Phase::Store, Request::Put);
            self.counters.bytes_out(Phase::Store, bytes.len());
            match self.store.put(key, bytes.clone(), opts).await {
                Ok(_) => return Ok(()),
                Err(StoreError::AlreadyExists) => {
                    self.counters.conflict(Phase::Store);
                    self.read_object_at(agent, key, dep).await?;
                    return Ok(());
                }
                Err(error) if error.is_retryable() && attempt < MAX_READ_ATTEMPTS => {
                    tracing::debug!(agent = %agent.to_hex(), %error, "agents object write retried");
                }
                Err(error) => {
                    tracing::warn!(agent = %agent.to_hex(), %error, "agents object write failed");
                    return Err(unavailable(agent, "dependency could not be written"));
                }
            }
        }
    }

    /// GET an object and refuse it unless its bytes hash to `dep`.
    pub(crate) async fn read_object_at(
        &self,
        agent: &AgentId,
        key: &str,
        dep: ObjectRef,
    ) -> Result<Bytes, AgentError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            self.counters.request(Phase::Store, Request::Get);
            match self.store.get(key, GetRange::Full).await {
                Ok(got) => {
                    self.counters.bytes_in(Phase::Store, got.data.len());
                    if ObjectRef::of(&got.data) != dep {
                        return Err(codec_error(
                            agent,
                            &CodecError::Invalid {
                                field: "object content hash",
                            },
                        ));
                    }
                    return Ok(got.data);
                }
                Err(error) if error.is_retryable() && attempt < MAX_READ_ATTEMPTS => {
                    tracing::debug!(agent = %agent.to_hex(), %error, "agents object read retried");
                }
                Err(StoreError::NotFound) => {
                    return Err(unavailable(agent, "referenced object is not in the store"));
                }
                Err(error) => {
                    tracing::warn!(agent = %agent.to_hex(), %error, "agents object read failed");
                    return Err(unavailable(agent, "object could not be read"));
                }
            }
        }
    }

    /// A referenced object the command did not supply must already exist,
    /// at the size the reference records.
    async fn probe_object(
        &self,
        agent: &AgentId,
        key: &str,
        dep: ObjectRef,
    ) -> Result<(), AgentError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            self.counters.request(Phase::Store, Request::Get);
            match self.store.head(key).await {
                Ok(meta) if meta.size == dep.size_bytes => return Ok(()),
                Ok(_) => {
                    return Err(codec_error(
                        agent,
                        &CodecError::Invalid {
                            field: "object size",
                        },
                    ));
                }
                Err(error)
                    if (error.is_retryable() || matches!(error, StoreError::NotFound))
                        && attempt < MAX_READ_ATTEMPTS => {}
                Err(StoreError::NotFound) => {
                    return Err(unavailable(agent, "referenced object is not in the store"));
                }
                Err(error) => {
                    tracing::warn!(agent = %agent.to_hex(), %error, "agents object probe failed");
                    return Err(unavailable(agent, "object could not be probed"));
                }
            }
        }
    }

    /// The advisory writes a successful CAS asks for. The rest of the
    /// effects are the runtime's and are returned in the [`Outcome`].
    async fn after_publish(&self, agent: &AgentId, effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::WriteDirEntry => {
                    wake::write_dir_entry(self.store.as_ref(), &self.counters, &self.tenant, agent)
                        .await;
                }
                Effect::WriteWakeHint { due_unix_ns } => {
                    wake::write_wake_hint(
                        self.store.as_ref(),
                        &self.counters,
                        &self.tenant,
                        agent,
                        *due_unix_ns,
                    )
                    .await;
                }
                Effect::WakeWorker
                | Effect::DispatchIntents { .. }
                | Effect::OperationTerminal { .. } => {}
            }
        }
    }
}
