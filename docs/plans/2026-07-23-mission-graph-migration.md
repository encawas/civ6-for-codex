# MissionGraph Runtime Migration Plan

Status: Proposed
Date: 2026-07-23

Architecture references:

- [MissionGraph Runtime SDD](../architecture/mission-graph-sdd.md)
- [ADR 0001: Explicit Runtime Composition](../adr/0001-explicit-runtime-composition.md)
- [ADR 0002: One StrategicContract Aggregate per Game Session](../adr/0002-strategic-contract-mission-graph.md)
- [ADR 0003: State Authority, Observation Completeness, and Module Boundaries](../adr/0003-state-authority-module-boundaries.md)
- [ADR 0004: TurnActionGraph and Wave/Barrier Execution](../adr/0004-turn-action-graph-execution.md)
- [ADR 0005: Atomic Strategic Proposal Decision and Research Authority Activation](../adr/0005-atomic-strategic-proposal-decision.md)

## 1. Migration Rules

```text
one Engine lineage
one bootstrap
one WorkflowStateStore
one StrategicContract root per game session
one write authority per Strategic Scope
one execution authority per action
no production dual writes
no shadow model calls
rollback at every phase boundary
```

The Authority Scope Set is the durable switch for strategic ownership. A
scope changes authority only in the transaction that records the active
StrategicContract revision and migration audit. Deployment alone never
implies a scope switch.

Deterministic validation rejects any `StrategicContractProposal` or
`MissionGraphPatch` that adds, modifies, or deletes a Scope outside the current
MissionGraph-owned Authority Scope Set. Legacy-owned Scopes may be read-only
facts or controlled context only; they cannot contain active Mission writes or
strategic-state changes. The sole exception is an atomic Scope
authority-switch transaction that updates the Authority Scope Set, initializes
new Scope Missions, records migration audit, stops the legacy write path, and
commits one new StrategicContract revision.

ActionAttempt verification is an independent Workflow audit fact. Any change
to Mission status, completion or invalidation, desired-outcome satisfaction,
or MissionGraph-persisted evidence must use an idempotent deterministic
Contract transition from the expected current revision through an atomic
WorkflowStateStorePort commit to one new revision. MissionGraph is never
updated in place, and external-only evidence cannot advance a Mission.

All durable Contract, MissionGraph, Patch, execution, request, attempt, and
approval state is accessed through one WorkflowStateStorePort and one database
authority.

## 2. Phase 0: Architecture Documentation

### Goal

Agree on target authority, boundaries, safety invariants, and staged migration
before implementation.

### Current authority

DecisionGap, PlannerRequest, Plan/PlanLease, PlanBundle, StoredTask, and
WorkflowEngine remain unchanged production authorities.

### Target authority

None is activated. The SDD and ADRs are proposed design records only.

### Allowed scope

Add the five approved architecture and migration Markdown documents.

### Data migration

None.

### Active object handling

No active object is changed, migrated, completed, or superseded.

### Tests

- Two architecture reviews check terminology and authority consistency.
- Relative links resolve.
- Changed-file scope contains exactly the five approved documents.
- Markdown whitespace passes `git diff --check`.

### Rollback

Revert the documentation commit. Runtime and persisted state are unaffected.

### Exit criteria

- Primary and secondary audits pass.
- All documents use canonical terms.
- No state or execution authority conflict remains.

### Deletion criteria

None.

### Explicitly not done

No Python, Schema, configuration, tests, runtime behavior, source modules, or
empty future interfaces.

## 3. Phase 1A: Generalize PlannerRequest

This phase completes before research authority switches.

### Goal

Allow a durable logical planning request to target StrategicContract creation
or MissionGraph repair without requiring DecisionGap.

### Current authority

PlannerRequest identity and lifecycle are tied to DecisionGap and
DecisionGroup compatibility references. ProviderAttempt and InformationRound
already provide durable model-call and information-continuation audit.

### Target authority

PlannerRequest remains the single logical model-call authority and supports:

```text
StrategicContract creation
MissionGraph repair
```

Legacy DecisionGap references are optional compatibility data, not required
identity for new Mission requests.

### Allowed scope

- Generalize PlannerRequest domain and Store operations.
- Add request targeting and revision identity for the two new semantics.
- Add deterministic request validation and recovery.
- Preserve ProviderAttempt, InformationRound, budget, and retry semantics.

### Data migration

- Existing rows remain readable without synthesized fields.
- Legacy `decision_gap_ids` remain intact.
- New identity can reference Contract ID, base Contract revision, Strategic
  Scope, Affected Mission Set, and patch base revision.
- No synthetic DecisionGap is created for a Mission request.

### Active object handling

- Legacy active requests continue under legacy rules.
- Before a later scope switch, each request completes, is superseded, or has
  an explicitly persisted migration decision.
- BACKOFF cannot be bypassed through migration.
- Persisted Provider responses recover without default model recall.

### Tests

- Mission requests persist without DecisionGap.
- Existing DecisionGap requests retain behavior.
- ProviderAttempt and InformationRound bind to both request forms.
- Versioning, deduplication, recovery, and ordinary call budgets remain safe.
- Reprocessing a persisted response does not call Provider again.

### Rollback

Disable new request targets while retaining compatible reads. Legacy requests
continue because their representation remains supported.

### Exit criteria

- Mission requests create no DecisionGap.
- Identity references Contract revision, scope, and patch base as applicable.
- Crash recovery does not require model recall.
- Ordinary legacy requests are unchanged.

### Deletion criteria

No old type or table is deleted. DecisionGap compatibility fields can be
removed only after every scope exits legacy planning authority.

### Explicitly not done

No scope switch, MissionGraph execution, second request system, Provider retry
redesign, or Agent hierarchy.

## 4. Phase 1B: Strategic Proposal Generation and Durable Human Wait

This section records the implementation delivered after Phase 1A and
supersedes PR 0's original assumption that Phase 1B would already switch
research authority.

```text
generalized PlannerRequest
-> ProviderAttempt / InformationRound
-> StrategicResearchProposal
-> StrategicProposalReadyTick
-> explicit-only AWAITING_HUMAN
```

### Goal

Persist a minimum single-Contract foundation and a validated, immutable
research Proposal with complete model-call audit and durable explicit-only
Human Wait, without making that Proposal effective.

### Current authority

At Phase 1B entry, DecisionGap and PlanLease decide research strategy and
StoredTask decides research execution. Phase 1A already allows PlannerRequest
to target StrategicContract creation or MissionGraph repair without a
synthetic DecisionGap.

### Target authority

Phase 1B adds durable candidate and audit state only:

```text
StrategicContract root/revision history = durable empty foundation
StrategicResearchProposal               = immutable candidate
explicit-only Human Wait                = interaction boundary
legacy DecisionGap / PlanLease           = research strategic authority
StoredTask                               = research execution authority
```

AuthorityScopeSet and MissionGraph remain empty. A Proposal does not activate
strategy.

### Allowed scope

- Add the minimum Contract, Mission, MissionGraph, AuthorityScopeSet, and
  ContractCommit domain shapes needed by the research slice.
- Persist one stable Contract root per game with immutable, atomically appended
  revisions and idempotent commit recovery.
- Keep non-empty AuthorityScopeSet and MissionGraph persistence disabled.
- Use Phase 1A PlannerRequest targets, ProviderAttempt, and InformationRound
  for strategic creation and repair requests.
- Validate and persist one immutable StrategicResearchProposal bound to its
  source Request, final successful Attempt, target Contract, expected base,
  source Observation, and canonical hash.
- Persist Proposal Ready, explicit resume request, resume/error, terminal
  request wait, RuntimeState, and Human Wait evidence with startup/replay
  aggregate validation.
- Preserve one WorkflowStateStore, one bootstrap, and one Engine lineage.

### Data migration

- Migrate existing databases to the Contract and Proposal schema without
  rewriting PlannerRequest, ProviderAttempt, InformationRound, DecisionGap,
  PlanLease, StoredTask, or execution authority.
- Export and import Contract roots, complete revision history, commits,
  Proposals, resume requests, RuntimeState, Human Wait context, and Tick audit.
- Reject non-empty MissionGraph or AuthorityScopeSet state at ordinary save,
  startup, and replay.
- Validate replay before deleting target-game data.

### Active object handling

- Legacy research DecisionGap, PlanLease, and StoredTask remain active under
  their existing rules.
- A Proposal Ready wait remains explicit-only across observation changes,
  execution-mode changes, repeated waiting Ticks, restart, and replay.
- Only an immutable explicit resume request can release the wait.
- Releasing the wait does not approve, reject, invalidate, apply, edit, or
  rebase the Proposal and does not call Provider again.
- Strategic Request failure terminal waits preserve dedicated termination,
  resume, error, and protected Tick-interval evidence.
- Public Store methods cannot advance strategic Request or InformationRound
  lifecycle outside the complete Runtime transaction.

### Tests

- One stable Contract root exists per game and revisions append idempotently.
- Stale bases, identity conflicts, partial commits, startup corruption, and
  replay corruption fail closed.
- AuthorityScopeSet and MissionGraph remain empty on every persisted revision.
- Strategic Request, Attempt, InformationRound, Proposal, Ready Tick, Runtime,
  Human Wait, and resume evidence round-trip stably.
- Proposal content and source evidence are immutable.
- Generic and terminal explicit-only waits survive repeated Ticks, restart,
  replay, concurrency, and impossible Tick-history attacks.
- Legacy research behavior and all existing tests remain unchanged.

### Rollback

Disable strategic Proposal request entry while retaining compatible reads and
audit. Existing Proposals remain inert. Contract foundation rows remain
historical data; legacy research authority continues without reconstruction.

### Exit criteria

- Phase 1A strategic Request targets are durable and replay-safe.
- Each game has at most one Contract root with immutable revision history.
- Persisted AuthorityScopeSet and MissionGraph remain empty.
- Valid strategic model responses can produce one immutable research Proposal.
- Proposal and strategic terminal waits are explicit-only, auditable, and
  restart/replay safe.
- No Proposal decision, Contract activation, authority switch, or
  Mission-derived StoredTask exists.

### Deletion criteria

Nothing is deleted. Contract and Proposal records are new durable audit;
legacy research types and tables remain authoritative and readable.

### Explicitly not done

No Proposal approval or rejection, system invalidation terminal, effective
Contract revision from Proposal, research authority switch, legacy research
write closure, Mission routing, Mission-derived StoredTask, StateDelta repair,
TurnActionGraph authority, BatchExecutor extraction, or other scope migration.

## 5. Phase 1C: Human Decision and Atomic Research Authority Activation

```text
StrategicResearchProposal
-> APPROVED / REJECTED / INVALIDATED
-> APPROVED: Contract revision and research authority commit atomically
-> dedicated Decision/Activation Tick completes
-> authoritative research routing continues
```

### Goal

Turn the Phase 1B immutable candidate into an auditable human decision and,
only for APPROVED, atomically make its research content effective while
transferring research write authority to MissionGraph.

### Current authority

StrategicResearchProposal and explicit-only wait are durable, but they are
inert. No ApprovalRecord decides the Proposal, no system invalidation fact
closes a stale Proposal, Contract revisions contain no Mission or owned scope,
and legacy research remains authoritative.

### Target authority

```text
StrategicResearchProposal          = immutable AI candidate
ApprovalRecord                     = sole human APPROVED/REJECTED authority
StrategicProposalInvalidatedTick   = sole system invalidation authority
StrategicContract revision         = effective research strategy
AuthorityScopeSet                  = current research write authority
StoredTask                         = temporary research execution authority
```

Other Strategic Scopes remain legacy-owned.

### Allowed scope

- Add Proposal-specific terminal decision contracts and complete evidence
  closure without adding mutable Proposal status.
- Extend ContractCommit with structured Proposal, hash, Approval, Request, and
  base-revision binding.
- Add atomic approve, reject, and invalidate transitions.
- Switch research AuthorityScopeSet ownership only with the approved Contract
  revision and research Mission.
- Stop legacy research writes after switch and route from the active
  Contract/Mission revision.
- Preserve StoredTask temporarily as execution authority, but never create it
  directly in the Proposal application transaction.
- Add user entry and Engine enablement only in the final rollout PR.

### PR 1C-1: Protocol and Persistence Foundation

- Constrain StrategicResearchProposal human decisions to APPROVED or REJECTED
  without narrowing unrelated approval workflows.
- Define Invalidated, Applied, and Rejected Tick contracts and terminal
  uniqueness.
- Add structured ContractCommit source binding.
- Add data contracts, Schema, canonical serialization, typed reads, replay
  import/export, ordinary-save checks, and shared startup/replay aggregate
  validators. Do not add a public operation that independently saves an
  ApprovalRecord or terminal Tick or performs a terminal decision.
- Add protocol and forged-state tests.
- Do not connect Engine, user actions, authority activation, or legacy write
  closure.

### PR 1C-2: Dormant Atomic Activation and Authority Projection

- Implement dedicated BEGIN IMMEDIATE approved, rejected, and invalidated
  full-aggregate services. No standalone ApprovalRecord or terminal Tick public
  save method may bypass them.
- Atomically persist Approval, Contract revision, ContractCommit, transition
  Tick, research Mission, AuthorityScopeSet, Runtime transition, and wait
  clearance as applicable.
- Close legacy research writes after ownership changes.
- Add active Contract/Mission routing projection and stale-revision guards.
- Add crash, restart, replay, concurrency, and idempotency coverage.
- Keep every new production path behind a dormant gate with no user or Engine
  caller.

### PR 1C-3: Runtime Entry and Enablement

- Add explicit user approve/reject actions.
- Integrate the existing Engine/Runtime lineage with the dedicated decision
  transition and post-activation routing.
- Remove dormancy only after Windows, Ubuntu, replay, concurrency, and
  end-to-end gates pass.
- Finalize documents and perform OpenSpec verify, sync, and archive after
  primary and secondary review.

### Data migration

- Add immutable Proposal decision and transition audit without rewriting
  Proposal content or existing Phase 1A Request evidence.
- Proposal-derived Contract revisions append to the one existing root and use
  revision = expected base + 1.
- The approved revision copies canonical Proposal objectives, constraints,
  source Observation, and research Mission.
- ContractCommit binds source Proposal ID/hash, Approval ID, PlannerRequest ID,
  and expected base structurally.
- Replay validates complete decision, activation, authority, Runtime/wait, and
  Tick evidence before deleting target-game data.

### Active object handling

- OPEN/REQUESTED legacy research DecisionGap completes or supersedes before
  authority switch.
- Active legacy PlannerRequest completes, supersedes, or follows an explicit
  migration path.
- Research PlanLease completes, invalidates, or blocks switch.
- Legacy READY StoredTask is cancelled, superseded, or otherwise made
  unclaimable before activation. If safe disposition cannot be proved,
  activation is blocked.
- VERIFYING StoredTask completes fresh verification before switch.
- UNCERTAIN ActionAttempt blocks authority switch and equivalent mutation until
  observed or human reconciliation.
- Pending legacy approval completes, invalidates, or resubmits against the new
  revision.
- Proposal decision and activation do not directly create StoredTask.
- After activation and the Decision/Activation Tick complete, later Routing
  reads the active Contract/Mission revision, performs a separate deterministic
  projection, and enters the normal Planner lifecycle before a replacement
  StoredTask may be created. Old provenance is not rewritten, audit association
  is retained, and at most one equivalent action is claimable.

### Tests

- Approve, reject, and stale invalidation produce their complete, mutually
  exclusive evidence sets.
- Identical decisions are idempotent; conflicting decisions fail.
- Two approvals produce one Approval, revision, and Applied Tick.
- Concurrent approve/reject is decided by the first committed transaction.
- Faults after Approval preparation, Contract preparation, or before the
  transition Tick leave no partial state.
- Startup and replay reject forged Approval, forged Contract, missing
  ContractCommit, wrong Proposal hash, missing Applied Tick, mixed terminal
  facts, duplicate Invalidated Ticks, and partial authority state.
- Replay failure occurs before target-game deletion.
- Research and Contract activate atomically; legacy research writes fail
  afterward.
- With research MissionGraph-owned and civic legacy-owned, MissionGraph-owned
  research strategic writes may pass scope validation, while writes targeting
  legacy-owned non-research scopes are rejected.
- Ordinary Ticks neither precede nor overlap the Decision/Activation Tick.
- Routing consumes only the active Contract/Mission revision; stale revisions
  cannot create claimable work.
- Proposal application creates no StoredTask and non-research scopes remain
  unchanged.
- The feature remains dormant until PR 1C-3.

### Rollback

Before PR 1C-3 enablement, disable the dormant gate or roll back code; no
persisted authority switch has occurred. After a game completes research
activation, Phase 1C provides no automatic reverse switch or revision
revocation. Pause new research work and require human handling. Any future
reverse migration requires a separate ADR and OpenSpec Change and must append a
new forward revision without deleting, modifying, or revoking an effective
revision.

### Exit criteria

- Each Proposal has exactly one derived OPEN, APPROVED, REJECTED, or
  INVALIDATED disposition from canonical durable facts.
- Approved content, Contract revision, ContractCommit, Applied Tick, research
  Mission, and AuthorityScopeSet are one atomic aggregate.
- Research strategy is MissionGraph-owned and legacy research writes are
  closed only after successful activation.
- Research execution remains solely StoredTask-owned until Phase 3.
- Startup, replay, concurrency, crash recovery, and protected Tick
  ordering pass.
- Non-research scopes are unchanged and no second Engine or Store exists.

### Deletion criteria

Delete no shared legacy type or table in Phase 1C. Research-specific legacy
writes may be removed only after rollback, historical reads, replay, and
control surfaces are proven. Shared structures remain while other scopes use
them.

### Explicitly not done

No Proposal editing, automatic approval, automatic rebase, revision revocation,
multi-approver or approval-permission system, non-research authority switch,
general MissionGraph editor, direct StoredTask creation from Proposal,
TurnActionGraph authority, BatchExecutor extraction, or general task
orchestration rewrite.

## 6. Phase 2: StateDelta and Local Mission Repair

### Goal

Detect meaningful game changes deterministically, calculate the Affected
Mission Set, and repair only the impacted closure by default.

### Current authority

Contract/MissionGraph owns research strategy. Observation processing does not
yet provide the full StateDelta and local repair contract.

### Target authority

Canonical NormalizedObservation remains current truth. Persisted compatible
Observation history is comparison baseline. Deterministic StateDelta and
MissionImpactAnalyzer select repair scope; committed Contract revision remains
strategic authority.

### Allowed scope

- Persist comparable history and completeness metadata.
- Add StateDeltaBuilder and MissionImpactAnalyzer behavior.
- Validate and commit MissionGraphPatch against base revision.
- Reuse PlannerRequest, ProviderAttempt, and InformationRound.

### Data migration

- Treat `initial_baseline` and `rebaseline_required` as Workflow control
  results, not ordinary StateDelta and not automatic baseline acceptance.
- Seed or replace the accepted baseline only with a persisted Canonical
  NormalizedObservation whose session is consistent, normalization and source
  versions are acceptable, and required collections and fields are complete.
- Version history by session, normalization version, and source version.
- Preserve Observation versus ActionAttempt evidence provenance.

### Active object handling

- Incompatible history produces `rebaseline_required`.
- An incomplete Observation never overwrites the last accepted baseline,
  manufactures deletion, switches Scope authority, deletes a Mission by
  itself, or by itself invalidates the Contract or rebuilds the full graph.
- Known independent comparable fields may produce local Delta while unknown
  portions remain unknown.
- Stale patch rejects or explicitly re-requests.
- Already committed patch recovers idempotently after crash.

### Tests

- Unknown/not-loaded never means deletion.
- Explicitly complete absence can mean deletion.
- Session/version mismatch, turn regression, and partial critical data
  rebaseline rather than fabricate Delta.
- A complete accepted baseline followed by an incomplete current Observation
  leaves the baseline unchanged; a later complete Observation does not classify
  reappearing data as newly created.
- Unrelated changes do not call Planner.
- Direct, downstream, conflict, and ancestor closures are selected.
- Duplicate patch handling neither increments revision nor recalls Provider.
- ActionAttempt references retain provenance.

### Rollback

Disable delta-triggered repair and continue from the last accepted Contract.
History remains inert audit data. Legacy authority is restored only through
the scope rollback protocol.

### Exit criteria

- Baseline and completeness invariants are covered.
- Affected closure repair is default.
- Full rebuild is explicit and audited.
- Patch commit is atomic, revision-checked, and idempotent.

### Deletion criteria

No shared execution type is deleted. Research-specific change detectors can
be removed only after replay proves parity.

### Explicitly not done

No graph database, event-sourcing framework, generic Patch engine, automatic
full rewrite, or expansion to unmigrated scopes.

## 7. Phase 3: TurnActionGraph

### Goal

Replace temporary StoredTask execution authority for research with a
revision-bound deterministic TurnActionGraph.

### Current authority

MissionGraph owns research strategy; StoredTask owns research execution.

### Target authority

`domain.Task` evolves into the canonical TurnActionNode contract and
TurnActionGraph becomes sole research execution authority.

### Allowed scope

- Add graph and node provenance that binds exactly one game session, one turn,
  one source Canonical NormalizedObservation, and one source Contract revision.
- Add TurnCompiler from current Contract, Mission, canonical Observation,
  action contracts, and approvals.
- Add stale-source checks before claim.
- Adapt existing execution without a second Engine.

### Data migration

- Map eligible research StoredTask to stable TurnActionNode identity only when
  source Mission, Contract revision, action, and target are provable.
- Preserve ActionAttempt references and historical task audit.
- Atomically switch research execution authority; never dual-write authorities.

### Active object handling

- READY StoredTask maps or cancels before switch.
- VERIFYING StoredTask finishes fresh verification first.
- UNCERTAIN ActionAttempt blocks an equivalent node.
- Pending approval rebinds to a current graph revision or invalidates.
- A turn change, Contract revision change, or inapplicable source Observation
  makes the graph stale; old READY nodes become unclaimable and Runtime
  recompiles before execution.

### Tests

- Graphs reference one session, one turn, one source Contract revision, one
  source Observation, and source Missions.
- Stable node identity survives restart and replay.
- Stale READY nodes never send.
- When verified action evidence changes Mission completion, Contract revision
  increments exactly once, duplicate recovery does not increment again, and
  the old TurnActionGraph cannot execute.
- A turn N graph with a future-turn candidate becomes unclaimable when turn
  N+1 begins; Runtime compiles a new graph before any execution.
- Action contracts and approvals are enforced.
- Equivalent StoredTask and TurnActionNode cannot both be claimable.
- ActionAttempt history remains continuous.

### Rollback

Stop claims, reconcile attempts, then atomically restore StoredTask execution
authority from a proven current Mission projection. Never activate both.

### Exit criteria

- TurnActionGraph alone decides research actions.
- `workflow_tasks` no longer decides research execution.
- Stale graph and duplicate-action tests pass.
- Replay and control surfaces read new execution state.

### Deletion criteria

Delete research StoredTask creation after rollback and historical reads are
validated. Shared `workflow_tasks` remains for unmigrated scopes.

### Explicitly not done

No BatchExecutor extraction, parallel mutation, generic DAG platform, or
other scope migration.

## 8. Phase 4: BatchExecutor

### Goal

Extract deterministic graph execution from the existing Engine while
preserving bounded mutation, durable attempts, and fresh verification.

### Current authority

TurnActionGraph is research execution authority, but WorkflowEngine still
coordinates execution steps.

### Target authority

BatchExecutor selects and executes one eligible node through Wave and the four
Barrier kinds. WorkflowRuntime remains sole orchestrator.

### Allowed scope

- Extract execution behavior from the current Engine in place.
- Reuse BoundedGamePort, ActionAttempt, verification, approval, and process
  lock boundaries.
- Implement Dependency, Verification, Approval, and Turn Barriers only.
- Enforce Turn Barrier by excluding future-turn nodes from the current Wave and
  compiling a new graph after reading and normalizing each new-turn
  Observation before reevaluating the Barrier.

### Data migration

No strategic authority changes. Persist only execution state needed to recover
node claim, attempt, Barrier, and verification through the one Store.

### Active object handling

- Recover PREPARED, SENT, VERIFYING, and UNCERTAIN ActionAttempt with existing
  rules.
- Never resend UNKNOWN automatically.
- Recheck Contract revision before each claim.
- Reject claims from graphs bound to a prior turn or an inapplicable source
  Observation.
- Keep active Barriers durable across restart.

### Tests

- At most one mutation occurs per Tick.
- Wave selection is deterministic and never sends in parallel.
- Every Barrier blocks and resumes correctly.
- A future-turn candidate in the turn N graph cannot be claimed in turn N+1;
  the turn N+1 Observation is normalized and a new graph is compiled first.
- Attempt persistence precedes delivery.
- Fresh canonical Observation drives verification.
- Approval, verification, and ActionAttempt audit recover across restart while
  execution claim still requires the current graph.
- BatchExecutor has no Planner dependency.
- Windows/Linux lock and replay behavior remain consistent.

### Rollback

Route graph nodes through the prior in-Engine deterministic path after active
attempts reconcile. Graph authority remains unchanged.

### Exit criteria

- Extracted executor matches replay and characterization behavior.
- One-mutation and UNKNOWN safety invariants pass.
- WorkflowRuntime invokes it only through application boundaries.

### Deletion criteria

Delete duplicated in-Engine execution only after all callers use
BatchExecutor and replay parity passes.

### Explicitly not done

No multi-mutation Tick, parallel game writes, distributed queue, generic
workflow language, Barrier plugins, or Planner fallback in Executor.

## 9. Phase 5: Migrate Remaining Strategic Scopes

Suggested order:

```text
civic
-> opening strategy
-> settler
-> city roles
-> diplomacy/trade
-> tactical/emergency
```

Research has already exercised the Phase 1C decision and activation protocol.
Each remaining scope repeats that authority protocol and uses the StateDelta,
MissionGraphPatch, TurnActionGraph, and BatchExecutor capabilities already
proven. Order changes require an explicit reviewed plan.

### Goal

Move each remaining scope from legacy to MissionGraph strategic authority and
TurnActionGraph execution authority without dual writes.

### Current authority

The Authority Scope Set records a mix of migrated and legacy scopes.

### Target authority

Each completed scope is MissionGraph-owned for strategy and
TurnActionGraph-owned for actions within the one game-session Contract.

### Allowed scope

One reviewed scope slice at a time, including its compiler, contracts,
migration audit, replay fixtures, and control-surface reads.

### Data migration

- Add a Scope only through the atomic authority-switch transaction: update the
  Authority Scope Set, initialize its Missions, record migration audit, stop
  its legacy write path, and commit one new StrategicContract revision.
- Migrate only provable durable intent and execution work.
- Retain compatibility reads until history validation completes.
- Stop legacy writes before deleting data or types.

### Active object handling

| Legacy object | Required switch handling |
| --- | --- |
| OPEN/REQUESTED DecisionGap | Complete or supersede before switch |
| Active PlannerRequest | Complete, supersede, or explicitly migrate |
| PlanLease | Invalidate, complete, or block switch |
| READY StoredTask | Cancel, supersede, or otherwise make the old task unclaimable before activation; block activation if safe disposition cannot be proved |
| VERIFYING StoredTask | Finish fresh verification before switch |
| UNCERTAIN ActionAttempt | Reconcile manually or from facts; never generate equivalent replacement mutation |
| Pending approval | Complete, invalidate, or resubmit on new revision |
| Provider BACKOFF | Preserve ordinary budget semantics; migration cannot bypass it |

An UNCERTAIN ActionAttempt blocks any switch that could generate the same
semantic mutation.

The authority-switch transaction does not create a Mission-derived StoredTask.
After activation and its dedicated Tick complete, later Routing may create a
replacement only through deterministic projection from the current Contract
and Mission revision and the normal Planner lifecycle. It records old-to-new
audit association, leaves the old task unclaimable, does not rewrite old
provenance, and leaves at most one equivalent action claimable.

### Tests

- Scope-specific safety and behavior characterization.
- Authority Scope Set has one writer per scope.
- With research MissionGraph-owned and civic legacy-owned, MissionGraph-owned
  research strategic writes may pass scope validation, while writes targeting
  legacy-owned non-research scopes are rejected.
- No equivalent legacy/new action is claimable.
- A legacy READY research task is cancelled, superseded, or made unclaimable
  before authority activation; any Mission-derived replacement can become
  claimable only after later Routing projection and the normal Planner lifecycle.
- Active-object migration and rollback fixtures.
- Multi-game isolation, replay, approvals, crash injection, and rebaseline.
- Unmigrated scopes remain unchanged.

### Rollback

Rollback one scope after attempts and approvals reconcile. Atomically return
it to compatible legacy authority without changing other scope ownership.

### Exit criteria

- Scope has one strategic and one execution authority.
- No production legacy write remains for it.
- Replay, recovery, and control surfaces understand migrated state.
- Rollback rehearsal passes.

### Deletion criteria

Delete only scope-specific write paths whose callers are gone. Shared types
and tables remain until no scope uses them.

### Explicitly not done

No bulk all-scope switch, production dual write, shadow Planner call, inferred
authority from deployment, or unrelated semantics redesign.

## 10. Phase 6: Delete Legacy Authorities

### Goal

Remove old strategic and execution authorities after every scope has migrated
and historical data is safe.

### Current authority

MissionGraph and TurnActionGraph own all production scopes/actions. Legacy
types and tables may remain for compatibility reads.

### Target authority

StrategicContract/MissionGraph is sole strategic authority and
TurnActionGraph/`domain.Task` is sole execution authority.

### Allowed scope

Delete legacy writes, callers, types, adapters, and finally tables in the
ordered sequence below.

### Data migration

1. Stop writes.
2. Retain compatibility reads.
3. Migrate and validate historical data.
4. Delete call sites.
5. Delete types.
6. Delete tables last.

### Active object handling

Deletion is blocked while any active DecisionGap, PlannerRequest compatibility
dependency, PlanLease, StoredTask authority, ActionAttempt reconciliation, or
approval relies on the old representation.

### Tests

- Historical database migration fixtures.
- Replay and control-panel compatibility.
- No production write query targets old plan/task authority.
- No Planner output uses `models.PlanBundle`.
- No action claim depends on StoredTask.
- Full safety, isolation, and crash recovery regression.

### Rollback

Before table deletion, restore compatibility readers and prior application
version. After table deletion, rollback requires a validated database backup
and reverse migration procedure.

### Exit criteria

- No scope creates DecisionGap or PlanLease.
- No production path writes old plan tables.
- No production path creates `models.PlanBundle`.
- StoredTask is not action authority.
- Replay and control surfaces support new state.
- Historical migration is verified.

### Deletion criteria

The exit criteria are the deletion gate. Tables are deleted only after code
and types no longer depend on them.

### Explicitly not done

No deletion merely because a replacement exists, no table-first cleanup, and
no loss of audit records required for recovery.

## 11. Phase 7: Shrink WorkflowRuntime

### Goal

Complete in-place convergence of WorkflowEngine into a small
WorkflowRuntime orchestrator.

### Current authority

New strategic and execution authorities are active, but transitional
coordination may remain in the large Engine.

### Target authority

WorkflowRuntime only:

- reads current state;
- orchestrates application services;
- persists Tick audit;
- selects the next phase.

### Allowed scope

Move remaining planning, delta, compilation, execution, and recovery behavior
behind established application boundaries without changing authority.

### Data migration

None unless runtime-state compatibility needs a versioned transition.
WorkflowStateStore remains the one state authority.

### Active object handling

Deployment resumes persisted workflow phases, attempts, approvals, requests,
and Barriers through existing recovery paths.

### Tests

- Every production entry point composes the same Runtime in `bootstrap.py`.
- Runtime imports no concrete SQLite or MCP implementation.
- Planner and Executor remain mutually independent.
- Replay, CLI, recording, loop, and control panel use one object graph.
- Import order does not select behavior.

### Rollback

Restore the prior orchestrator while retaining the same ports, state authority,
and persisted domain representation.

### Exit criteria

- Runtime only orchestrates.
- Application services own use-case behavior.
- Bootstrap is the only composition root.
- No second Engine remains.
- Legacy coordination code has no caller.

### Deletion criteria

Delete old WorkflowEngine methods only after production entry points and
replay use the converged Runtime and characterization tests pass.

### Explicitly not done

No second runtime, service mesh, deployment redesign, UI redesign, or generic
agent platform.

## 12. Program Completion

Migration completes only when one StrategicContract root governs each game
session, every Strategic Scope has MissionGraph authority, every action has
TurnActionGraph authority, and Runtime is one explicitly composed
orchestrator. Compatibility readers may outlive write authority only for a
bounded, audited migration interval.
