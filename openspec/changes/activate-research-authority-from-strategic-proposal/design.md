## Context

See [proposal.md](proposal.md) for motivation. This design is based on the implementation at the Phase 1A and Phase 1B merge baseline, not on the earlier PR 0 rollout assumption.

Phase 1A generalized PlannerRequest targets so strategic creation and repair no longer require synthetic DecisionGap identities while retaining ProviderAttempt and InformationRound evidence. Phase 1B then established one revisioned StrategicContract root per game and durable replay, but the foundation intentionally rejects every persisted non-empty AuthorityScopeSet and MissionGraph. The latest Phase 1B slice persists immutable StrategicResearchProposal candidates, their source Request and final ProviderAttempt evidence, a StrategicProposalReadyTick, and an explicit-only AWAITING_HUMAN lifecycle. Explicit resume currently releases the wait without approving or applying the Proposal.

Two existing shared models need Proposal-specific constraints in later implementation. The generic ApprovalRecord supports decisions beyond APPROVED and REJECTED, while StrategicContractCommit does not yet carry structured Proposal provenance. Phase 1C narrows the research Proposal protocol without changing unrelated approval workflows and extends the one Contract commit model rather than creating parallel records.

## Goals / Non-Goals

**Goals:**

- Give every Proposal lifecycle fact one durable authority and define complete evidence for APPROVED, REJECTED, and INVALIDATED.
- Commit approved strategic content and research authority in one serializable transaction.
- Make ordinary writes, startup, and replay validate the same evidence closure and Tick ordering.
- Preserve idempotency, crash recovery, replay stability, and one writer per scope across a three-PR dormant rollout.
- Keep the existing WorkflowStateStore as the only database authority.

**Non-Goals:**

- Editing or rebasing Proposals, revoking committed revisions, automatic approval, multi-approver workflow, or an approval permission system.
- Activating non-research scopes, creating a second MissionGraph, or redesigning generic task orchestration.
- Creating StoredTask in a Proposal decision transaction.
- Changing production behavior in this documentation PR.

## Decisions

### 1. Derive terminal state from mutually exclusive durable facts

StrategicResearchProposal stays immutable. Its disposition is derived as follows:

| Durable facts | Derived disposition |
| --- | --- |
| No ApprovalRecord and no Invalidated Tick | OPEN |
| One matching APPROVED ApprovalRecord | APPROVED |
| One matching REJECTED ApprovalRecord | REJECTED |
| One matching StrategicProposalInvalidatedTick | INVALIDATED |

ApprovalRecord is the sole human decision authority. The research Proposal protocol accepts only APPROVED and REJECTED; shared approval decisions used by other workflows do not become legal here. StrategicProposalInvalidatedTick is the sole system invalidation authority. It records stale base or target facts and never impersonates a human rejection.

The two terminal families are mutually exclusive. Applied, Rejected, Resume, RuntimeState, and Human Wait records prove transitions or interaction state but cannot establish a human decision.

Alternatives rejected:

- Mutable status, approved, rejected, or invalidated fields on Proposal would create conflicting fact sources.
- Using AppliedTick as approval authority would make crash recovery unable to distinguish human intent from transition completion.
- Treating stale base as REJECTED would forge a human decision.

### 2. Use complete evidence sets for each terminal disposition

The accepted terminal evidence is:

```text
APPROVED
= Proposal
+ APPROVED ApprovalRecord
+ StrategicContractCommit
+ StrategicContract revision
+ StrategicProposalAppliedTick

REJECTED
= Proposal
+ REJECTED ApprovalRecord
+ StrategicProposalRejectedTick

INVALIDATED
= Proposal
+ StrategicProposalInvalidatedTick
```

REJECTED and INVALIDATED have no Proposal-derived Contract revision. APPROVED has no Invalidated Tick. Each Proposal has at most one terminal ApprovalRecord or one Invalidated Tick.

StrategicContract.approval_status remains an immutable redundant snapshot. Proposal-derived revisions use APPROVED and require the matching ApprovalRecord. Foundation revisions may use NOT_REQUIRED. The snapshot cannot create approval by itself.

### 3. Serialize each terminal transition with BEGIN IMMEDIATE

Approval, rejection, and invalidation acquire the SQLite writer boundary before re-reading mutable aggregate state. The logical approval transaction is:

```text
BEGIN IMMEDIATE
1. Re-read Proposal and canonical hash.
2. Prove no human terminal decision and no system invalidation.
3. Validate source PlannerRequest, final ProviderAttempt, Ready Tick, and explicit-only wait.
4. Re-read the active Contract root and compare target identity and expected base revision.
5. Insert APPROVED ApprovalRecord.
6. Append StrategicContract revision = expected base + 1 from Proposal content.
7. Insert structured StrategicContractCommit.
8. Insert StrategicProposalAppliedTick.
9. Transfer research AuthorityScopeSet ownership to MissionGraph.
10. Set Runtime to ROUTING and clear Human Wait context.
COMMIT
```

The Contract revision, MissionGraph Mission, and research authority change are one aggregate write. A fault at any point rolls everything back.

Rejection performs one transaction that writes REJECTED ApprovalRecord and StrategicProposalRejectedTick, resumes ROUTING, and clears the wait without touching Contract or authority.

Invalidation performs one transaction that proves the base or target stale, writes StrategicProposalInvalidatedTick, resumes ROUTING, and clears the wait. It writes no ApprovalRecord, Contract revision, or Provider retry.

Alternative rejected: committing Contract first and switching authority on a later Tick exposes two writers or effective strategy without declared authority.

### 4. Bind Contract provenance structurally

A Proposal-derived StrategicContractCommit carries:

```text
source_proposal_id
source_proposal_hash
source_approval_id
source_planner_request_id
expected_base_revision
```

These are typed fields in the durable commit contract. The existing reason remains descriptive and cannot substitute for any identity. Startup and replay compare every source field with Proposal, Approval, Request, Contract, and Applied Tick evidence.

Alternative rejected: parsing a reason string is not canonical, typed, or safe under replay.

### 5. Validate the same aggregate on write, startup, and replay

One aggregate validator covers terminal uniqueness, source binding, revision continuity, approval snapshot consistency, activation completeness, authority ownership, Runtime/wait state, and protected Tick intervals.

Replay prepares and validates the complete incoming game aggregate together with retained external rows before deleting target-game data. Any forged Approval, missing ContractCommit, wrong Proposal hash, missing Applied Tick, mixed terminal facts, partial authority switch, or impossible Tick ordering fails before replacement.

Committed activation recovery is idempotent. The same decision identity returns the existing evidence and never appends a revision or calls the Provider again. Conflicting decisions fail. Two concurrent approvals produce one revision; concurrent approve/reject is decided by the first transaction to commit.

### 6. Treat the decision Tick as an exclusive transition interval

The ordering is:

```text
explicit-only Proposal wait
-> dedicated Decision/Activation Tick
-> Tick completes
-> Runtime ROUTING
-> ordinary Tick
```

No ordinary Tick starts before or overlaps the dedicated transition Tick. No ordinary read can observe Approval without Contract, authority, and transition evidence because all become visible at commit. Startup and replay validate the complete interval rather than trusting a caller-supplied starting Runtime state.

### 7. Switch research authority without creating executable work

The approval transaction changes only research ownership. Legacy research DecisionGap, PlanLease, strategy-state, and equivalent write paths become fail-closed or read-only once the active AuthorityScopeSet includes research. Non-research scope ownership and state remain unchanged.

Proposal application does not create StoredTask. A later Routing step reads the active Contract and authoritative research Mission, performs a separate deterministic revision-bound projection, and enters the normal PlannerRequest lifecycle. Stale Contract or Mission revisions cannot produce claimable work.

Alternative rejected: direct StoredTask creation would conflate candidate content, effective strategy, and execution authority.

### 8. Roll out through one active Change

- **PR 1C-1:** add Proposal-specific decision contracts, terminal Tick contracts, structured ContractCommit provenance, persistence shapes, and fail-closed validators. Do not connect Engine or a user entry point.
- **PR 1C-2:** implement atomic approve/reject/invalidate services, research AuthorityScopeSet activation, legacy research write closure, authoritative routing projection, and crash/concurrency/replay behavior behind a dormant gate.
- **PR 1C-3:** add the user entry point and Engine integration, remove dormancy only after end-to-end verification, update final documents, then verify, sync, and archive this Change.

OpenSpec remains active through PR 1C-1 and PR 1C-2. It is development guidance, not runtime authority.

## Risks / Trade-offs

- **[Shared ApprovalRecord currently permits extra decisions]** -> Add Proposal-specific validation at every write, startup, and replay boundary without narrowing unrelated workflows.
- **[Foundation Store rejects non-empty scope and Mission state]** -> Remove that guard only inside PR 1C-2's dormant atomic activation path after PR 1C-1 validators exist.
- **[Partial legacy write closure could create dual authority]** -> Key every research write path from the persisted AuthorityScopeSet and test legacy entry points fail after switch.
- **[Replay may accept a state ordinary persistence cannot create]** -> Reuse one aggregate validator and preflight before target deletion.
- **[Decision and ordinary Tick overlap could expose impossible history]** -> Validate the whole protected interval and serialize Runtime transition under the same process and database boundaries.
- **[Dormant code can drift before enablement]** -> Keep one OpenSpec Change and require PR 1C-3 end-to-end tests before removing the gate.
- **[Rollback after mutations can duplicate work]** -> Block rollback while ActionAttempt or approval state is unresolved and require a proven compatible legacy baseline.

## Migration Plan

1. Merge this documentation-only PR from the Phase 1A/1B implementation baseline. It changes no runtime behavior.
2. Land PR 1C-1 protocol and persistence foundations while the Phase 1B explicit-only wait remains the production behavior.
3. Land PR 1C-2 atomic services and research projection dormant. Existing games remain legacy-owned until an explicit activation can commit.
4. In PR 1C-3, expose the user decision entry point, integrate Engine routing, run migration/replay/concurrency end-to-end checks, and enable research activation.
5. Keep non-research scopes unchanged. Keep legacy audit reads until exit and deletion criteria are met.
6. For rollback, stop new research claims, reconcile attempts and approvals, prove a compatible legacy baseline, and atomically restore legacy ownership. Never dual-write.

This Change is not synced or archived during steps 1-3. PR 1C-3 performs final verification, sync, and archive only after primary and secondary review.
