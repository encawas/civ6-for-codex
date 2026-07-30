# ADR 0005: Atomic Strategic Proposal Decision and Research Authority Activation

- Status: Proposed
- Date: 2026-07-30
- Scope: StrategicResearchProposal terminal authority, Contract activation, research scope cutover

Related documents:

- [MissionGraph Runtime SDD](../architecture/mission-graph-sdd.md)
- [MissionGraph Runtime Migration Plan](../plans/2026-07-23-mission-graph-migration.md)
- [Phase 1C OpenSpec Change](../../openspec/changes/activate-research-authority-from-strategic-proposal/proposal.md)

## Context

Phase 1A generalized PlannerRequest targets while preserving ProviderAttempt and InformationRound audit. Phase 1B then added the single StrategicContract persistence foundation and durable StrategicResearchProposal generation. A valid Proposal is currently an immutable candidate bound to its source Request, final ProviderAttempt, expected Contract base, and Proposal Ready Tick. Runtime enters an explicit-only Human Wait that survives ordinary waiting Ticks, restart, and replay.

That boundary is intentionally incomplete. Explicit resume releases the wait but does not approve or reject the Proposal, append a Contract revision, populate MissionGraph, transfer research authority, or create StoredTask. The Contract foundation also rejects non-empty AuthorityScopeSet and MissionGraph state until a later controlled activation path exists. Legacy DecisionGap, PlanLease, and StoredTask paths therefore remain authoritative for research.

Phase 1C needs one durable answer to five separate questions:

1. What immutable content did the model propose?
2. What terminal decision did a human make?
3. Did system facts make the Proposal permanently stale?
4. Which strategic content is effective?
5. Which implementation currently owns research writes?

Using one mutable status or one transition Tick for all five would make crash recovery, replay, and authority cutover ambiguous.

## Decision

### Proposal remains immutable

StrategicResearchProposal remains the canonical immutable AI candidate. It gains no mutable status, approved, rejected, invalidated, applied, or replacement fields. Its terminal disposition is derived from separate durable evidence.

### Human and system terminal facts have separate authorities

ApprovalRecord is the sole authority for a human Proposal decision. The StrategicResearchProposal decision protocol permits only APPROVED and REJECTED, even if shared approval infrastructure supports other decisions for other workflows.

StrategicProposalInvalidatedTick is the sole authority for permanent invalidation caused by system facts such as BASE_REVISION_CHANGED, TARGET_CONTRACT_CHANGED, or TARGET_CONTRACT_CREATED. Invalidation is not a human decision, writes no ApprovalRecord, and is not represented as REJECTED.

A Proposal has at most one human terminal ApprovalRecord or one StrategicProposalInvalidatedTick. APPROVED plus REJECTED, either human result plus INVALIDATED, or multiple Invalidated Ticks are invalid.

Applied, Rejected, Resume, RuntimeState, and Human Wait records prove transition or interaction state. None can establish the human decision.

### Contract approval status is a redundant snapshot

A Proposal-derived Contract revision records approval_status=APPROVED as an immutable redundant snapshot of its matching APPROVED ApprovalRecord. The field is not approval authority. Foundation revisions may use NOT_REQUIRED. Effective revisions do not use REQUIRED or REJECTED, and a rejected Proposal creates no Contract revision.

### Contract provenance is structural

A Proposal-derived StrategicContractCommit binds these facts as typed data:

```text
source_proposal_id
source_proposal_hash
source_approval_id
source_planner_request_id
expected_base_revision
```

A descriptive reason may supplement this data but cannot replace it. Startup and replay compare the structured fields with the Proposal, ApprovalRecord, PlannerRequest, Contract revision, and Applied Tick.

### Approval and research activation are one transaction

Approval executes under BEGIN IMMEDIATE and re-reads all mutable aggregate state before writing:

```text
1. Re-read Proposal and validate its canonical hash.
2. Prove no human terminal decision and no system invalidation.
3. Validate source PlannerRequest, final ProviderAttempt, Ready Tick, and explicit-only wait.
4. Re-read the Contract head and compare target identity and expected base.
5. Write ApprovalRecord(APPROVED).
6. Append Contract revision = expected base + 1 from Proposal content.
7. Write the structured StrategicContractCommit.
8. Write StrategicProposalAppliedTick.
9. Transfer research AuthorityScopeSet ownership to MissionGraph.
10. Set Runtime to ROUTING and clear Human Wait context.
11. Commit.
```

The ApprovalRecord, Contract revision, ContractCommit, Applied Tick, research Mission, AuthorityScopeSet change, Runtime transition, and wait clearance all commit or all roll back. No observer can see an approved Proposal without its effective Contract or see MissionGraph research content while legacy research still owns writes.

A stale target discovered in step 4 produces INVALIDATED through its own atomic transaction. It does not write Approval or automatically rebase or recall the Provider.

Rejection atomically writes ApprovalRecord(REJECTED), StrategicProposalRejectedTick, Runtime ROUTING, and wait clearance. It does not modify Contract or authority.

### Transition completion precedes ordinary work

The required order is:

```text
Proposal explicit-only wait
-> dedicated Decision/Activation Tick
-> Tick completion
-> Runtime ROUTING
-> ordinary Tick
```

Ordinary Routing, Observing, planning, or execution Ticks neither precede nor overlap the dedicated transition. Startup and replay validate the protected interval rather than trusting a Tick's claimed starting Runtime state.

### Proposal application creates no StoredTask

The approval transaction establishes effective strategy and research write authority only. It does not create StoredTask.

Later Routing reads the active approved Contract and authoritative research Mission, performs a separate deterministic revision-bound projection, and enters the normal PlannerRequest lifecycle before executable work can exist. Proposal or Applied Tick identity alone cannot produce claimable work.

### Startup and replay fail closed

Ordinary persistence, database startup, and replay reuse the same aggregate invariants. Complete evidence is:

```text
APPROVED
= Proposal + APPROVED ApprovalRecord + ContractCommit
+ Contract revision + StrategicProposalAppliedTick

REJECTED
= Proposal + REJECTED ApprovalRecord + StrategicProposalRejectedTick

INVALIDATED
= Proposal + StrategicProposalInvalidatedTick
```

Replay validates incoming evidence together with retained state before deleting target-game data. A forged Approval, missing ContractCommit, wrong Proposal hash, missing Applied Tick, mixed terminal state, partial authority switch, or impossible Tick interval fails before replacement.

Identical repeated decisions are idempotent. Conflicting decisions fail. Concurrent decisions serialize at BEGIN IMMEDIATE; the first committed terminal result wins.

## Consequences

### Positive

- Proposal content, human intent, system invalidation, effective strategy, and runtime authority remain distinct and auditable.
- Approval cannot expose a partial Contract or dual research writers.
- Restart and replay can prove one complete activation instead of inferring it from mutable status.
- Stale Proposals are classified honestly as system-invalidated rather than human-rejected.
- Existing Phase 1A/1B Request, Attempt, InformationRound, Proposal, and wait evidence remains useful.
- Non-research scopes remain unchanged.

### Negative

- Approval requires a larger aggregate validator and transaction than a status update.
- Proposal-specific constraints must narrow the shared ApprovalRecord decision set without breaking unrelated approval workflows.
- The current foundation guard against non-empty scope/Mission state cannot be relaxed until dormant atomic activation and validators land together.
- Legacy research write entry points must all consult persisted AuthorityScopeSet before enablement.
- Three PRs carry one active specification and require disciplined dormancy until the final integration.

## Rejected Alternatives

### Add status, approved, or invalidated fields to Proposal

This creates mutable Proposal content and competing terminal fact sources.

### Use Applied Tick instead of ApprovalRecord

Applied Tick proves transition completion, not human intent. It cannot serve as the sole approval authority across partial failure and replay.

### Record a stale Proposal as REJECTED

A stale base is a system fact, not a human choice. It must produce INVALIDATED without ApprovalRecord.

### Commit Contract and switch authority on the next Tick

The intermediate state exposes effective strategy without matching write ownership or allows both legacy and MissionGraph research writers.

### Bind provenance only in a reason string

Free-form text is not typed, canonical, or reliably validated during replay.

### Create StoredTask during approval

This conflates candidate handling, effective strategy, routing projection, and execution authority.

### Add another approval model or state store

WorkflowStateStore and the existing approval lineage remain the only persistence boundary. Proposal-specific constraints extend them instead of creating parallel authority.

## Dormant Rollout

- **PR 1C-1:** protocol and persistence foundation. Add Proposal-specific terminal contracts, structured ContractCommit binding, and fail-closed validators. Do not connect Engine or user entry.
- **PR 1C-2:** dormant atomic services and authority projection. Add approve/reject/invalidate transactions, research authority cutover, legacy write closure, routing projection, and recovery tests behind a dormant gate.
- **PR 1C-3:** runtime entry and enablement. Add user actions and Engine integration, remove dormancy after end-to-end validation, finalize documents, then verify, sync, and archive the OpenSpec Change.

The OpenSpec Change remains active through PR 1C-1 and PR 1C-2. No Phase 1C production behavior is enabled before PR 1C-3.
