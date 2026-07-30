## Why

Phase 1A and Phase 1B now provide generalized PlannerRequest targets, a single revisioned StrategicContract persistence root, immutable research Proposal evidence, and an explicit-only durable human wait. The remaining boundary is deliberately absent: no durable human decision can yet activate a Proposal, commit its strategic content, or transfer research write authority away from the legacy workflow.

Phase 1C must define that boundary before implementation so three follow-up PRs share one authority model and cannot accidentally introduce mutable Proposal state, partial activation, dual research writers, or a second approval system.

## What Changes

- Define immutable Proposal terminal state as a derivation from one of two mutually exclusive durable facts: an `ApprovalRecord` for human `APPROVED` or `REJECTED`, or a `StrategicProposalInvalidatedTick` for system invalidation.
- Define atomic approval activation that appends exactly one StrategicContract revision and switches the research AuthorityScopeSet in the same transaction.
- Require structured ContractCommit provenance binding to Proposal identity and hash, Approval identity, source PlannerRequest, and expected base revision.
- Define fail-closed startup and replay evidence validation, concurrency and idempotency behavior, crash recovery, and explicit-only wait transition ordering.
- Stop legacy research writes only when the atomic authority switch commits; leave all non-research scopes under their existing authority.
- Require the locked approval transaction to atomically dispose safely cancellable legacy research execution and block activation for claimable, in-flight, verifying, uncertain, or otherwise revivable old work.
- Add grouped Contract/Mission provenance to the existing StoredTask representation in PR 1C-1 so later routing, claim, retry, confirmation, recovery, startup, and replay can distinguish legacy, current, and stale research execution without another task table.
- Migrate Phase 1B OPEN Proposals whose waits were already resumed to deterministic system invalidation before enablement, using one source-bound causal timestamp rather than inventing historical user time, and prohibit generic Proposal resume after APPROVE/REJECT becomes available.
- Restrict Proposal-derived activation to the same immutable `ACTIVE` research Mission across Proposal, Contract revision, ContractCommit, Applied Tick, routing, and task provenance; the only executable mapping is `research -> set_research`, never a selector in desired_outcome or free JSON.
- Keep Proposal application separate from StoredTask creation. Later routing consumes the committed MissionGraph revision and performs an independent deterministic projection.
- Replace the PR 0 Phase 1B implementation assumption with the actual Phase 1A/1B baseline and a three-PR Phase 1C rollout:
  - PR 1C-1: protocol, persistence, and grouped StoredTask provenance foundation, with no routing, Engine, or user entry behavior.
  - PR 1C-2: dormant atomic decision/activation, provenance-emitting authority projection, legacy write closure, guarded task mutations, and recovery validation.
  - PR 1C-3: Phase 1B released-wait migration, APPROVE/REJECT entry, generic Proposal-resume closure, Engine integration, enablement, end-to-end verification, and final OpenSpec sync/archive.

No runtime behavior changes in this documentation PR. The Change remains active and all implementation tasks remain unchecked.

## Capabilities

### New Capabilities

- `strategic-proposal-decision`: Immutable Proposal disposition, human/system authority separation, terminal uniqueness, stale invalidation, Phase 1B released-wait migration, idempotency, and conflict behavior.
- `strategic-contract-activation`: Atomic approved Contract revision activation, structured provenance, complete evidence closure, and startup/replay validation.
- `research-authority-routing`: Atomic research authority transfer, legacy write closure, durable StoredTask provenance, guarded post-cutover task mutations, post-transition Tick ordering, authoritative routing, and dormant rollout.

### Modified Capabilities

None. This repository has no previously published OpenSpec capabilities.

## Impact

- **Current authority:** legacy DecisionGap, PlanLease, and StoredTask paths still own research behavior; persisted StrategicResearchProposal objects are immutable candidates waiting for explicit handling.
- **Target authority:** approved StrategicContract revisions own effective research strategy and AuthorityScopeSet identifies MissionGraph as the sole research writer; activated content contains the Proposal-derived `ACTIVE` research Mission with the fixed `set_research` execution mapping, while rejected or invalidated Proposals never create a Contract revision.
- **Affected scope:** research only. Civic, production, unit, city, settler, diplomacy, tactical, and all other scopes retain their current authority and cannot be bound as Proposal-derived research activation provenance.
- **Persistence:** PR 1C-1 extends the existing StoredTask/workflow_tasks representation with one nullable all-or-none Contract/Mission provenance group and keeps legacy rows all null. Future work remains inside WorkflowStateStore and the replay stream; it must not add another task table, repository, database authority, or mutable Proposal record.
- **Rollback boundary:** before PR 1C-3 enablement, disable the dormant gate or roll back code without changing persisted authority. After a game activates research, Phase 1C provides no automatic reverse switch or revision revocation; new research work pauses for human handling. Any future reverse migration requires a separate ADR and OpenSpec Change and appends a new forward revision without deleting, modifying, or revoking an effective revision.
- **Dependencies:** the change builds on the Phase 1A PlannerRequest lifecycle and Phase 1B Contract/Proposal/wait audit. StrategicProposalWaitResumedTick remains historical interaction evidence and is never reinterpreted as human approval.

## Non-goals

- Proposal editing, automatic approval or rebase, approval permissions, multi-approver workflow, or revoking an effective Contract revision.
- A second MissionGraph, approval model, Store, runtime, or Proposal status source.
- Non-research authority migration, generic MissionGraph editing, or generic task orchestration.
- Direct StoredTask creation from Proposal application.
