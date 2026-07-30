## Why

Phase 1A and Phase 1B now provide generalized PlannerRequest targets, a single revisioned StrategicContract persistence root, immutable research Proposal evidence, and an explicit-only durable human wait. The remaining boundary is deliberately absent: no durable human decision can yet activate a Proposal, commit its strategic content, or transfer research write authority away from the legacy workflow.

Phase 1C must define that boundary before implementation so three follow-up PRs share one authority model and cannot accidentally introduce mutable Proposal state, partial activation, dual research writers, or a second approval system.

## What Changes

- Define immutable Proposal terminal state as a derivation from one of two mutually exclusive durable facts: an `ApprovalRecord` for human `APPROVED` or `REJECTED`, or a `StrategicProposalInvalidatedTick` for system invalidation.
- Define atomic approval activation that appends exactly one StrategicContract revision and switches the research AuthorityScopeSet in the same transaction.
- Require structured ContractCommit provenance binding to Proposal identity and hash, Approval identity, source PlannerRequest, and expected base revision.
- Define fail-closed startup and replay evidence validation, concurrency and idempotency behavior, crash recovery, and explicit-only wait transition ordering.
- Stop legacy research writes only when the atomic authority switch commits; leave all non-research scopes under their existing authority.
- Keep Proposal application separate from StoredTask creation. Later routing consumes the committed MissionGraph revision and performs an independent deterministic projection.
- Replace the PR 0 Phase 1B implementation assumption with the actual Phase 1A/1B baseline and a three-PR Phase 1C rollout:
  - PR 1C-1: protocol and persistence foundation, with no Engine or user entry point.
  - PR 1C-2: dormant atomic decision/activation, authority projection, legacy write closure, and recovery validation.
  - PR 1C-3: user entry point, Engine integration, enablement, end-to-end verification, and final OpenSpec sync/archive.

No runtime behavior changes in this documentation PR. The Change remains active and all implementation tasks remain unchecked.

## Capabilities

### New Capabilities

- `strategic-proposal-decision`: Immutable Proposal disposition, human/system authority separation, terminal uniqueness, stale invalidation, idempotency, and conflict behavior.
- `strategic-contract-activation`: Atomic approved Contract revision activation, structured provenance, complete evidence closure, and startup/replay validation.
- `research-authority-routing`: Atomic research authority transfer, legacy write closure, post-transition Tick ordering, authoritative routing, and dormant rollout.

### Modified Capabilities

None. This repository has no previously published OpenSpec capabilities.

## Impact

- **Current authority:** legacy DecisionGap, PlanLease, and StoredTask paths still own research behavior; persisted StrategicResearchProposal objects are immutable candidates waiting for explicit handling.
- **Target authority:** approved StrategicContract revisions own effective research strategy and AuthorityScopeSet identifies MissionGraph as the sole research writer; rejected or invalidated Proposals never create a Contract revision.
- **Affected scope:** research only. Civic, settler, city, diplomacy, tactical, and all other scopes retain their current authority.
- **Persistence:** future work extends the existing WorkflowStateStore and replay stream. It must not add another repository, database authority, or mutable Proposal record.
- **Rollback:** before enablement, the feature remains dormant. After cutover, rollback first drains or reconciles active attempts and approvals, then atomically restores a proven legacy research baseline without dual writes.
- **Dependencies:** the change builds on the Phase 1A PlannerRequest lifecycle and Phase 1B Contract/Proposal/wait audit. It does not replace their evidence.

## Non-goals

- Proposal editing, automatic approval or rebase, approval permissions, multi-approver workflow, or revoking an effective Contract revision.
- A second MissionGraph, approval model, Store, runtime, or Proposal status source.
- Non-research authority migration, generic MissionGraph editing, or generic task orchestration.
- Direct StoredTask creation from Proposal application.
