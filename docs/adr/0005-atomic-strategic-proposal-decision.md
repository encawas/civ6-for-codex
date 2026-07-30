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
5. Re-read every legacy research StoredTask, all associated ActionAttempts, and pending task confirmation.
6. Move PENDING, READY, BLOCKED, FAILED, ESCALATED, and AWAITING_CONFIRMATION tasks to CANCELLED; close legacy confirmations and audit their prior state plus authority-switch reason.
7. Block and roll back for RUNNING, VERIFYING, or UNCERTAIN tasks or any associated PREPARED, VERIFYING, or UNCERTAIN ActionAttempt.
8. Prove no legacy research work remains claimable, in flight, verifying, uncertain, or revivable.
9. Write ApprovalRecord(APPROVED).
10. Append Contract revision = expected base + 1 from Proposal content.
11. Write the structured StrategicContractCommit.
12. Write StrategicProposalAppliedTick with legacy execution disposition audit.
13. Transfer research AuthorityScopeSet ownership to MissionGraph.
14. Set Runtime to ROUTING and clear Human Wait context.
15. Commit.
```

The legacy execution disposition, ApprovalRecord, Contract revision, ContractCommit, Applied Tick, research Mission, AuthorityScopeSet change, Runtime transition, and wait clearance all commit or all roll back. No observer can see an approved Proposal without its effective Contract or see MissionGraph research content while legacy research remains claimable.

The cutover matrix is deterministic: PENDING, READY, BLOCKED, FAILED, ESCALATED, and AWAITING_CONFIRMATION tasks become permanently non-revivable CANCELLED tasks in the transaction; the associated legacy confirmation is closed. RUNNING, VERIFYING, and UNCERTAIN tasks block activation, as does any associated ActionAttempt in PREPARED, VERIFYING, or UNCERTAIN. DONE, CANCELLED, and EXPIRED tasks remain inert history when no unresolved Attempt exists. Preflight cleanup cannot authorize activation; the re-read under BEGIN IMMEDIATE is authoritative. Startup and replay reject switched authority with any claimable, in-flight, verifying, uncertain, or revivable legacy research execution.

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

### Phase 1B released waits migrate before enablement

StrategicProposalWaitResumedTick is historical interaction evidence, not approval. Before PR 1C-3 enables decisions, one writer-locked migration classifies each historical OPEN Proposal:

| Phase 1B history | Required result |
| --- | --- |
| Valid unresolved Proposal-ready wait | Remains OPEN and decidable through APPROVE or REJECT |
| WaitResumedTick with no terminal fact | INVALIDATED with PRE_PHASE1C_WAIT_RELEASED |
| Stale target or expected base | INVALIDATED with the existing deterministic stale reason |
| No Proposal | No operation |

The migration is idempotent for one or multiple Proposals and writes no ApprovalRecord, Contract revision, ContractCommit, MissionGraph authority, StoredTask, or Provider call. After enablement, request_human_resume rejects strategic_contract_proposal_ready; that wait can leave only through APPROVE or REJECT. Supported non-Proposal waits keep their existing resume behavior.

Startup and replay can read the pre-enable history only as migration input. Enabled ordinary work cannot begin while an OPEN Proposal already has a WaitResumedTick.

### Proposal application creates no StoredTask

The approval transaction establishes effective strategy and research write authority only. It does not create StoredTask.

Later Routing reads the active approved Contract and authoritative research Mission, performs a separate deterministic revision-bound projection, and enters the normal PlannerRequest lifecycle before executable work can exist. Proposal or Applied Tick identity alone cannot produce claimable work.

PR 1C-1 adds one optional all-or-none provenance group to the existing StoredTask and workflow_tasks representation:

```text
source_contract_id
source_contract_revision
source_mission_id
source_mission_revision
```

Legacy rows migrate with all four fields null. Before first cutover, provenance-free set_research is explicitly legacy. Mission-derived set_research has all four fields and matches the active same-game Contract and research Mission revisions. PR 1C-2 makes claim, retry, confirmation release, and recovery revalidate those facts in each write transaction. Partial, stale, cross-game, or post-cutover null provenance cannot become claimable. No second task model or table is created.

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

Replay validates incoming evidence together with retained state before deleting target-game data. A forged Approval, missing ContractCommit, wrong Proposal hash, missing Applied Tick, mixed terminal state, partial authority switch, impossible Tick interval, partial task provenance, stale claimable research task, or enabled OPEN Proposal with a historical WaitResumedTick fails before replacement.

PR 1C-1 defines decision evidence and grouped StoredTask provenance data contracts, Schema, canonical serialization, reads, import/export, and ordinary-save/startup/replay validation. It changes no routing or task lifecycle behavior, performs no terminal decision, and exposes no standalone public ApprovalRecord or terminal Tick save method. PR 1C-2 adds dedicated full-aggregate decision entry points plus dormant provenance-emitting routing and task mutation guards. PR 1C-3 migrates Phase 1B released waits before enabling APPROVE/REJECT and disabling generic Proposal resume.

## Consequences

### Positive

- Proposal content, human intent, system invalidation, effective strategy, and runtime authority remain distinct and auditable.
- Approval cannot expose a partial Contract or dual research writers.
- Restart and replay can prove one complete activation instead of inferring it from mutable status.
- Stale Proposals are classified honestly as system-invalidated rather than human-rejected.
- Existing Phase 1A/1B Request, Attempt, InformationRound, Proposal, and wait evidence remains useful.
- Historical Resume remains auditable without being reinterpreted as human intent.
- Persisted task provenance lets restart and replay distinguish legacy, current, and stale research execution.
- Non-research scopes remain unchanged.

### Negative

- Approval requires a larger aggregate validator and transaction than a status update.
- Proposal-specific constraints must narrow the shared ApprovalRecord decision set without breaking unrelated approval workflows.
- The current foundation guard against non-empty scope/Mission state cannot be relaxed until dormant atomic activation and validators land together.
- Legacy research write entry points must all consult persisted AuthorityScopeSet before enablement.
- StoredTask persistence gains four nullable grouped fields before dormant routing can consume them.
- PR 1C-3 requires an idempotent compatibility migration before decisions can be enabled.
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

### Infer task provenance from action type or caller path

action_type identifies research intent but cannot prove a Contract or Mission revision after restart or replay. plan_id and call path are not durable authority.

### Interpret a Phase 1B Resume Tick as approval

Resume only released a wait. Treating it as approval would fabricate human intent; leaving the Proposal OPEN after the wait is gone would bypass the decision protocol.

### Add another approval model or state store

WorkflowStateStore and the existing approval lineage remain the only persistence boundary. Proposal-specific constraints extend them instead of creating parallel authority.

## Dormant Rollout

- **PR 1C-1:** protocol and persistence foundation. Add Proposal terminal contracts and grouped StoredTask Contract/Mission provenance Domain/Schema/serialization/replay support. Keep routing, task mutations, Engine, and user behavior unchanged.
- **PR 1C-2:** dormant atomic services and authority projection. Add full-aggregate decisions, research cutover, legacy closure, provenance-emitting routing, and transaction-local task guards behind a dormant gate. No standalone ApprovalRecord or terminal Tick save method may bypass them.
- **PR 1C-3:** runtime entry and enablement. Install the official Verify Skill, migrate released Phase 1B Proposal waits, reject generic Proposal resume, add APPROVE/REJECT and Engine integration, then verify, sync, and archive after end-to-end validation.

The OpenSpec Change remains active through PR 1C-1 and PR 1C-2. No Phase 1C production behavior is enabled before PR 1C-3.

Before PR 1C-3 enablement, the dormant gate or code may be disabled without a persisted authority change. Once a game activates research, Phase 1C provides no automatic reverse switch or revision revocation. New research work pauses for human handling; any future reverse migration requires a separate ADR and OpenSpec Change and a new forward revision.
