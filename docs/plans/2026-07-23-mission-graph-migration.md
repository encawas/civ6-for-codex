# MissionGraph Runtime Migration Plan

Status: Proposed
Date: 2026-07-23

Architecture references:

- [MissionGraph Runtime SDD](../architecture/mission-graph-sdd.md)
- [ADR 0001: Explicit Runtime Composition](../adr/0001-explicit-runtime-composition.md)
- [ADR 0002: One StrategicContract Aggregate per Game Session](../adr/0002-strategic-contract-mission-graph.md)
- [ADR 0003: State Authority, Observation Completeness, and Module Boundaries](../adr/0003-state-authority-module-boundaries.md)
- [ADR 0004: TurnActionGraph and Wave/Barrier Execution](../adr/0004-turn-action-graph-execution.md)

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

## 4. Phase 1B: Research Strategic Vertical Slice

```text
research_direction_required
-> StrategicContract research Mission
-> set_research
```

### Goal

Prove one scope can use MissionGraph strategic authority within the single
game-session Contract while preserving current execution safety.

### Current authority

DecisionGap and PlanLease decide research strategy. StoredTask is execution
authority.

### Target authority

```text
MissionGraph = research strategic authority
StoredTask   = temporary research execution authority
```

The Contract root covers the game session. Other scopes remain legacy-owned.

### Allowed scope

- Add minimum Contract/Mission domain and typed Store Port operations.
- Create or repair research through generalized PlannerRequest.
- Deterministically project `set_research` StoredTask from current Mission.
- Route through existing bootstrap and Engine lineage.

### Data migration

- Create the game-session Contract and Authority Scope Set.
- Switch only research atomically to MissionGraph authority.
- Persist source Contract/Mission revision on projected execution work.
- Stop new research DecisionGap and PlanLease writes in the same switch.

### Active object handling

- OPEN/REQUESTED research DecisionGap completes or supersedes.
- Active PlannerRequest completes, supersedes, or explicitly migrates.
- Research PlanLease completes, invalidates, or blocks switch.
- READY StoredTask continues only with proven source and target, else cancels.
- VERIFYING StoredTask completes fresh verification first.
- UNCERTAIN ActionAttempt blocks equivalent replacement and authority switch
  until human or observed reconciliation.
- Pending approval completes, invalidates, or resubmits on the new revision.

### Tests

- New and existing games have one Contract root.
- Only research enters MissionGraph authority.
- Research creates no DecisionGap or PlanLease after switch.
- StoredTask projection is deterministic and revision-bound.
- Stale Mission revisions cannot produce claimable tasks.
- Other scopes remain legacy and unchanged.
- Replay and crash recovery cover the slice.

### Rollback

Drain or reconcile research ActionAttempts and approvals, then atomically
switch research back to a compatible legacy baseline. Never dual-write.

### Exit criteria

- Research strategy is only MissionGraph-owned.
- Research execution is only StoredTask-owned.
- No second Engine exists.
- Provider, approval, attempt, and fresh-verification tests pass.
- Other scopes are unchanged.

### Deletion criteria

Delete research legacy writes only after rollback and historical reads are
proven. Shared types remain while other scopes use them.

### Explicitly not done

No other scope migration, StateDelta repair, TurnActionGraph authority,
BatchExecutor extraction, or speculative full Contract Schema.

## 5. Phase 2: StateDelta and Local Mission Repair

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

- Seed `initial_baseline` from first accepted canonical Observation.
- Version history by session, normalization version, and source version.
- Preserve Observation versus ActionAttempt evidence provenance.

### Active object handling

- Incompatible history produces `rebaseline_required`.
- Rebaseline does not automatically invalidate the full graph.
- Stale patch rejects or explicitly re-requests.
- Already committed patch recovers idempotently after crash.

### Tests

- Unknown/not-loaded never means deletion.
- Explicitly complete absence can mean deletion.
- Session/version mismatch, turn regression, and partial critical data
  rebaseline rather than fabricate Delta.
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

## 6. Phase 3: TurnActionGraph

### Goal

Replace temporary StoredTask execution authority for research with a
revision-bound deterministic TurnActionGraph.

### Current authority

MissionGraph owns research strategy; StoredTask owns research execution.

### Target authority

`domain.Task` evolves into the canonical TurnActionNode contract and
TurnActionGraph becomes sole research execution authority.

### Allowed scope

- Add graph and node revision/provenance semantics needed for research.
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
- Nodes from stale Contract revisions invalidate or recompile.

### Tests

- Graphs reference Contract revision, Observation, and source Missions.
- Stable node identity survives restart and replay.
- Stale READY nodes never send.
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

## 7. Phase 4: BatchExecutor

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

### Data migration

No strategic authority changes. Persist only execution state needed to recover
node claim, attempt, Barrier, and verification through the one Store.

### Active object handling

- Recover PREPARED, SENT, VERIFYING, and UNCERTAIN ActionAttempt with existing
  rules.
- Never resend UNKNOWN automatically.
- Recheck Contract revision before each claim.
- Keep active Barriers durable across restart.

### Tests

- At most one mutation occurs per Tick.
- Wave selection is deterministic and never sends in parallel.
- Every Barrier blocks and resumes correctly.
- Attempt persistence precedes delivery.
- Fresh canonical Observation drives verification.
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

## 8. Phase 5: Migrate Remaining Strategic Scopes

Suggested order:

```text
research
-> civic
-> opening strategy
-> settler
-> city roles
-> diplomacy/trade
-> tactical/emergency
```

Each scope repeats the Phase 1B authority protocol and uses the StateDelta,
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

- Add scope to MissionGraph authority atomically with Contract revision.
- Migrate only provable durable intent and execution work.
- Retain compatibility reads until history validation completes.
- Stop legacy writes before deleting data or types.

### Active object handling

| Legacy object | Required switch handling |
| --- | --- |
| OPEN/REQUESTED DecisionGap | Complete or supersede before switch |
| Active PlannerRequest | Complete, supersede, or explicitly migrate |
| PlanLease | Invalidate, complete, or block switch |
| READY StoredTask | Continue with proven source/target or cancel |
| VERIFYING StoredTask | Finish fresh verification before switch |
| UNCERTAIN ActionAttempt | Reconcile manually or from facts; never generate equivalent replacement mutation |
| Pending approval | Complete, invalidate, or resubmit on new revision |
| Provider BACKOFF | Preserve ordinary budget semantics; migration cannot bypass it |

An UNCERTAIN ActionAttempt blocks any switch that could generate the same
semantic mutation.

### Tests

- Scope-specific safety and behavior characterization.
- Authority Scope Set has one writer per scope.
- No equivalent legacy/new action is claimable.
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

## 9. Phase 6: Delete Legacy Authorities

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

## 10. Phase 7: Shrink WorkflowRuntime

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

## 11. Program Completion

Migration completes only when one StrategicContract root governs each game
session, every Strategic Scope has MissionGraph authority, every action has
TurnActionGraph authority, and Runtime is one explicitly composed
orchestrator. Compatibility readers may outlive write authority only for a
bounded, audited migration interval.
