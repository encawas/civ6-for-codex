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
5. Re-read every legacy research StoredTask, all associated ActionAttempts, and pending task confirmation.
6. Move PENDING, READY, BLOCKED, FAILED, ESCALATED, and AWAITING_CONFIRMATION tasks to CANCELLED; close legacy confirmations and record prior state plus authority-switch reason.
7. If a task is RUNNING, VERIFYING, or UNCERTAIN, or any Attempt is PREPARED, VERIFYING, or UNCERTAIN, roll back and block activation.
8. Prove no legacy research work remains claimable, in flight, verifying, uncertain, or revivable.
9. Insert APPROVED ApprovalRecord.
10. Append StrategicContract revision = expected base + 1 from Proposal content.
11. Insert structured StrategicContractCommit.
12. Insert StrategicProposalAppliedTick with legacy execution disposition audit.
13. Transfer research AuthorityScopeSet ownership to MissionGraph.
14. Set Runtime to ROUTING and clear Human Wait context.
COMMIT
```

Legacy execution disposition, the Contract revision, MissionGraph Mission, and research authority change are one aggregate write. A fault at any point rolls everything back. Preflight cleanup is advisory only; the locked re-read and disposition are authoritative.

| Existing legacy research state | Treatment while approval holds the writer lock |
| --- | --- |
| PENDING, READY, BLOCKED, FAILED, ESCALATED | Move to CANCELLED and audit the previous state and authority-switch reason |
| AWAITING_CONFIRMATION | Move the task to CANCELLED and close the legacy confirmation without treating it as Contract approval |
| RUNNING | Block activation until recovery proves the mutation boundary is resolved |
| VERIFYING | Block activation until fresh verification completes |
| UNCERTAIN | Block activation until fact-based or human reconciliation completes |
| DONE, CANCELLED, EXPIRED | Preserve as inert history when no unresolved Attempt exists |

CANCELLED is permanently non-revivable for this cutover. After authority transfer, every legacy research task-creation, retry, release, and confirmation path reads AuthorityScopeSet and fails closed. Any ActionAttempt in PREPARED, VERIFYING, or UNCERTAIN blocks activation regardless of the task status. Startup and replay enforce the same closure.

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
source_mission_id
source_mission_revision
```

These are typed fields in the durable commit contract. StrategicProposalAppliedTick binds the same Mission identity and revision. The Proposal Mission, activated Contract Mission, ContractCommit, and Applied Tick must all resolve to one immutable Mission with `scope=research` and `status=ACTIVE`. Startup and replay compare every source field with Proposal, Approval, Request, Contract, Mission, and Applied Tick evidence.

The action semantic is not free-form provenance. A closed domain mapping assigns `research -> set_research`; `desired_outcome`, tool-name strings, and free JSON cannot select or override the action. Civic, production, unit, city, every non-ACTIVE Mission, and every non-`set_research` action fail the aggregate before any activation fact commits.

Alternative rejected: parsing a reason string is not canonical, typed, or safe under replay.

### 5. Validate the same aggregate on write, startup, and replay

One aggregate validator covers terminal uniqueness, source binding, revision continuity, approval snapshot consistency, activation completeness, research Mission scope/status/action semantics, authority ownership, Runtime/wait state, migration-origin causal time, and protected Tick intervals.

Replay prepares and validates the complete incoming game aggregate together with retained external rows before deleting target-game data. Any forged Approval, missing ContractCommit, wrong Proposal hash, missing Applied Tick, mixed terminal facts, non-ACTIVE or non-research Mission, non-`set_research` action, partial authority switch, or impossible migration/Tick ordering fails before replacement.

PR 1C-1 adds only the data contracts, Schema, canonical serialization, typed reads, import/export, ordinary-save checks, and startup/replay aggregate validation needed to recognize these facts. It also adds four nullable, grouped provenance fields to the existing StoredTask/workflow_tasks representation: source_contract_id, source_contract_revision, source_mission_id, and source_mission_revision. Existing rows migrate with all four null; partial groups fail validation. PR 1C-1 does not change routing, claim, retry, confirmation, or recovery behavior.

PR 1C-1 may reject forged or incomplete persisted states, but it does not implement a real decision transaction and exposes no public operation that can independently save an ApprovalRecord or terminal Tick or move a Proposal to APPROVED, REJECTED, or INVALIDATED.

#### PR 1C-1 concrete implementation record

Workflow database v11 implements this foundation by adding the four nullable provenance columns to the existing `workflow_tasks` table. Proposal-specific approvals remain in `approval_records`, the three typed terminal outcomes remain in `workflow_ticks`, and Proposal provenance remains in canonical `StrategicContractCommit` data. No parallel terminal table, Repository, Store, or task model is introduced.

Ordinary writes, startup, and replay preflight call the same Proposal/Contract/task aggregate validator. Public Store operations reject standalone strategic Proposal ApprovalRecord, Proposal-derived ContractCommit, and Applied/Rejected/Invalidated Tick writes. Engine, Human Wait, research routing, task claim, retry, confirmation, and recovery remain unchanged. The implementation therefore has no reviewed protocol deviation; reuse of the existing authority tables is the intended single-source design.

PR 1C-2 introduces three dedicated full-aggregate entry points for approved, rejected, and invalidated outcomes. Only those atomic operations implement same-decision idempotency, conflicting-decision rejection, stale invalidation, and concurrent decision behavior. The same committed decision identity returns existing evidence and never appends a revision or calls the Provider again. Two concurrent approvals produce one revision; concurrent approve/reject is decided by the first transaction to commit.

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

### 7. Migrate Phase 1B released Proposal waits before enablement

StrategicProposalWaitResumedTick remains historical interaction evidence and is never reinterpreted as human approval. PR 1C-3 runs one deterministic migration under the WorkflowStateStore writer boundary before enabling decisions:

| Historical Phase 1B state | Phase 1C treatment |
| --- | --- |
| OPEN with a valid unresolved Proposal-ready wait | Preserve OPEN and allow APPROVE or REJECT |
| OPEN with StrategicProposalWaitResumedTick and no terminal fact | Write one StrategicProposalInvalidatedTick with PRE_PHASE1C_WAIT_RELEASED |
| OPEN with stale Contract target or base | Use the existing deterministic stale invalidation reason |
| No Proposal | No operation |

The migration writes no ApprovalRecord, Contract revision, ContractCommit, MissionGraph authority, StoredTask, or Provider call. It handles multiple historical Proposals independently and is idempotent. Startup and replay may recognize the pre-enable state as migration input, but enabled ordinary work starts only after no OPEN Proposal retains a WaitResumedTick.

A migration-origin Invalidated Tick carries typed origin `PHASE1C_ENABLEMENT_MIGRATION` and structured bindings to ProposalReadyTick, ResumeRequest, and StrategicProposalWaitResumedTick. Its logical `started_at` and `completed_at` are identical and equal one microsecond after the maximum of Proposal.created_at, source PlannerRequest.completed_at, final ProviderAttempt.completed_at, ProposalReadyTick.completed_at, ResumeRequest.requested_at, and StrategicProposalWaitResumedTick.completed_at. The value is derived only from durable Phase 1B evidence; it is not a guessed historical user-operation or current wall-clock time.

Ordinary save, startup, and replay use one validator to resolve the bindings and recompute the exact timestamp. Missing sources, mismatched identities, an earlier or merely different timestamp, or a non-representable successor fails before mutation or replay deletion. A successful migration reopens normally and preserves the same Tick through export, empty-store import, and re-export.

After enablement, strategic_contract_proposal_ready can leave AWAITING_HUMAN only through the dedicated APPROVE or REJECT aggregate transition. request_human_resume rejects Proposal waits but retains existing behavior for supported non-Proposal waits.

Alternative rejected: treating an old Resume Tick as approval would fabricate human intent; leaving it OPEN would create a Proposal with no decision wait.

### 8. Switch research authority without creating executable work

The approval transaction changes only research ownership. Legacy research DecisionGap, PlanLease, strategy-state, and equivalent write paths become fail-closed or read-only once the active AuthorityScopeSet includes research. Non-research scope ownership and state remain unchanged.

Proposal application does not create StoredTask. A later Routing step reads the active Contract and authoritative research Mission, performs a separate deterministic revision-bound projection, and enters the normal PlannerRequest lifecycle.

Mission-derived research StoredTask uses the four-field provenance group added in PR 1C-1. Before first cutover, set_research with all four fields null is explicitly legacy. After cutover, a set_research task is eligible only when all four fields are present and match the same-game active Contract and an `ACTIVE` Mission whose scope is exactly `research`. ContractCommit and Applied Tick bind that same Mission identity/revision. Routing obtains the action only from the closed `research -> set_research` mapping; neither Mission desired_outcome nor arbitrary strings/JSON can choose an operation. Each claim, retry, confirmation release, and recovery operation re-reads and validates the active aggregate in its own write transaction; null, partial, stale, cross-game, non-research, non-ACTIVE, or non-`set_research` provenance cannot become claimable.

Alternative rejected: direct StoredTask creation would conflate candidate content, effective strategy, routing projection, and execution authority.

Alternative rejected: inferring provenance from action_type, plan_id, or caller path cannot survive restart and replay.

### 9. Roll out through one active Change

- **PR 1C-1:** add Proposal-specific decision and terminal Tick contracts plus the grouped StoredTask Contract/Mission provenance Domain/Schema/serialization/replay foundation. It provides no public terminal transition, routing, claim guard, Engine, or user entry behavior.
- **PR 1C-2:** implement dedicated atomic approved, rejected, and invalidated full-aggregate services, research AuthorityScopeSet activation, legacy research write closure, provenance-emitting routing and transaction-local claim guards, and crash/concurrency/replay behavior behind a dormant gate. No standalone ApprovalRecord or terminal Tick public save method is permitted.
- **PR 1C-3:** install the official generated openspec-verify-change Skill, migrate Phase 1B released waits, reject generic Proposal resume, add the user decision entry and Engine integration, remove dormancy only after end-to-end verification, update final documents, then verify, sync, and archive this Change.

OpenSpec remains active through PR 1C-1 and PR 1C-2. It is development guidance, not runtime authority.

PR 1C-2 is implemented behind a Store-local dormant gate with no Engine,
bootstrap, or user caller. Its dedicated transactions, persisted authority
checks, revision-bound research projection, task mutation guards, and shared
startup/replay validation implement this design without enabling production
decisions. PR 1C-3 remains responsible for compatibility migration, user entry,
Runtime integration, and removal of dormancy.

## Risks / Trade-offs

- **[Shared ApprovalRecord currently permits extra decisions]** -> Add Proposal-specific validation at every write, startup, and replay boundary without narrowing unrelated workflows.
- **[Foundation Store rejects non-empty scope and Mission state]** -> Remove that guard only inside PR 1C-2's dormant atomic activation path after PR 1C-1 validators exist.
- **[Partial legacy write closure could create dual authority]** -> Key every research write path from the persisted AuthorityScopeSet and test legacy entry points fail after switch.
- **[Replay may accept a state ordinary persistence cannot create]** -> Reuse one aggregate validator and preflight before target deletion.
- **[Decision and ordinary Tick overlap could expose impossible history]** -> Validate the whole protected interval and serialize Runtime transition under the same process and database boundaries.
- **[StoredTask origin is not currently persisted]** -> Add one grouped provenance contract in PR 1C-1 and require revision validation in every post-cutover task mutation transaction.
- **[Phase 1B Resume can leave an OPEN Proposal without a wait]** -> Migrate historical released waits to system invalidation and reject generic Proposal resume before enabling decisions.
- **[Migration could invent an impossible historical time]** -> Bind migration-origin invalidation to durable Phase 1B sources and derive one canonical logical timestamp that every validation path recomputes.
- **[Generic Mission data could escape the research slice]** -> Require the Proposal-derived ACTIVE research Mission and the closed set_research mapping across Proposal, Contract, commit, Applied Tick, routing, and task provenance.
- **[Dormant code can drift before enablement]** -> Keep one OpenSpec Change and require PR 1C-3 end-to-end tests before removing the gate.
- **[An activated game cannot safely return to legacy authority in Phase 1C]** -> Pause new research work and require human handling. Any future reverse migration needs a separate ADR and OpenSpec Change and must append a new forward revision without deleting, modifying, or revoking an effective revision.

## Migration Plan

1. Merge this documentation-only PR from the Phase 1A/1B implementation baseline. It changes no runtime behavior.
2. Land PR 1C-1 protocol, persistence, and grouped StoredTask provenance foundations while Phase 1B wait and task behavior remains unchanged.
3. Land PR 1C-2 atomic services, provenance-emitting research projection, and task mutation guards dormant. Existing games remain legacy-owned until an explicit activation can commit.
4. In PR 1C-3, migrate Phase 1B released Proposal waits, disable generic Proposal resume, expose APPROVE/REJECT, integrate Engine routing, run migration/replay/concurrency end-to-end checks, and enable research activation.
5. Keep non-research scopes unchanged. Keep legacy audit reads until exit and deletion criteria are met.
6. Before PR 1C-3 enablement, disable the dormant gate or roll back code without changing persisted authority. After a game activates research, Phase 1C offers no automatic reverse switch or revision revocation; pause new research work and require human handling. A future reverse migration requires a separate ADR and OpenSpec Change and a new forward revision.

This Change is not synced or archived during steps 1-3. PR 1C-3 performs final verification, sync, and archive only after primary and secondary review.
