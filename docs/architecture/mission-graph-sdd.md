# MissionGraph Runtime Software Design Description

Status: Proposed
Date: 2026-07-23

Related documents:

- [ADR 0001: Explicit Runtime Composition](../adr/0001-explicit-runtime-composition.md)
- [ADR 0002: One StrategicContract Aggregate per Game Session](../adr/0002-strategic-contract-mission-graph.md)
- [ADR 0003: State Authority, Observation Completeness, and Module Boundaries](../adr/0003-state-authority-module-boundaries.md)
- [ADR 0004: TurnActionGraph and Wave/Barrier Execution](../adr/0004-turn-action-graph-execution.md)
- [ADR 0005: Atomic Strategic Proposal Decision and Research Authority Activation](../adr/0005-atomic-strategic-proposal-decision.md)
- [MissionGraph Runtime Migration Plan](../plans/2026-07-23-mission-graph-migration.md)

## 1. Purpose

This document defines the target structure and migration boundaries for
converging the current durable decision and task workflow into a
StrategicContract and MissionGraph runtime. It defines concepts and
responsibility boundaries, not Python classes, Pydantic fields, database
columns, or a provider topology.

```text
StrategicContract
    `-- MissionGraph
            |
        StateDelta
            |
     Affected Mission Set
            |
       MissionGraphPatch
            |
      TurnActionGraph
            |
   BatchExecutor + Wave/Barrier
```

## 2. Current System Baseline

| Current component | Current responsibility | Target disposition |
| --- | --- | --- |
| `bootstrap.py` | Sole production composition root | Retain |
| `WorkflowEngine` | Coordinates Ticks, rules, planning, execution, and recovery | Incrementally shrink in place into `WorkflowRuntime` |
| `DecisionGap` | Durable unresolved strategic question | Migrate by Strategic Scope, then retire as strategic authority |
| `PlannerRequest` | Durable logical planning request with legacy and strategic targets | Generalized in Phase 1A; retain |
| `ProviderAttempt` | Audit of calls across the model boundary | Retain |
| `InformationRound` | Declarative information-gathering continuation | Retain |
| `StrategicContract` / `MissionGraph` | One revisioned aggregate root; Phase 1B persistence currently permits only empty scope and Mission state | Retain and activate by reviewed scope |
| `StrategicResearchProposal` | Immutable research candidate bound to Request, Attempt, Contract base, and Observation | Retain; add terminal decision and activation evidence in Phase 1C |
| `Plan` / `PlanLease` | Current durable intent and validity | Replace by scope with StrategicContract/MissionGraph |
| `models.PlanBundle` | Legacy Planner output and task collection | Delete after migration adapters retire |
| `models.StoredTask` | Current production execution task | Retain through Phase 2, including after Phase 1C research activation; retire in Phase 3 |
| `domain.Task` | New domain task model | Evolve into the canonical TurnActionGraph node contract |
| `ActionAttempt` | Action delivery, recovery, and verification audit | Retain |
| Rules / Progression | Compile deterministic tasks and events | Retain; never make them a new state authority |
| `strategy_state` and legacy plan tables | Legacy planning state | Stop writes by scope, migrate data, then delete |
| `workflow_tasks` | Legacy execution task table | Retire as execution authority after Phase 3 |
| `workflow_ticks` | Tick audit | Retain |

The repository currently contains legacy types in `models.py` and newer types
in `domain/`. MissionGraph migration must converge those types. It must not
add a third long-lived Plan, Task, or Runtime state model.

The implemented baseline is now Phase 1A plus Phase 1B. Phase 1A generalized
PlannerRequest targets without replacing ProviderAttempt or InformationRound.
Phase 1B established the one-Contract persistence root and durable replay, then
added immutable StrategicResearchProposal generation and explicit-only Human
Wait recovery. It deliberately did not approve or apply a Proposal, persist a
non-empty Authority Scope Set or MissionGraph, switch research authority, or
project Mission-derived StoredTask. Legacy research planning and execution
remain authoritative until Phase 1C atomically changes that ownership.

PR 1C-1 now supplies the version 11 terminal-evidence and grouped StoredTask
provenance foundation. PR 1C-2 supplies dormant atomic approve, reject, and
invalidate transactions, research authority activation, legacy research write
closure, and a revision-bound set_research projection. These capabilities have
no Engine, bootstrap, or user caller. Therefore the deployed behavior remains
Phase 1B until PR 1C-3 performs compatibility migration and deliberately
enables the existing Runtime lineage.

## 3. System Invariants

1. A game session has exactly one active StrategicContract revision.
2. Strategic Scope is an authoritative partition inside that Contract, not an
   independently rooted Contract.
3. Each Strategic Scope has exactly one write authority at a time.
4. Each executable action has exactly one execution authority at a time.
5. Canonical NormalizedObservation is the sole authority for current game
   facts.
6. WorkflowStateStore is the sole authority for workflow state.
7. Planner output is a proposal until deterministic validation and atomic
   persistence succeed.
8. Planner does not access the game or Store.
9. BatchExecutor does not call a model.
10. WorkflowRuntime orchestrates and contains no adapter-specific SQL or MCP
    calls.
11. Migration uses one Engine lineage and one bootstrap.
12. The first execution phases retain at most one mutation per Tick.

## 4. StrategicContract Cardinality and Scope Authority

StrategicContract is the logical strategic root aggregate for one
`game_session`. Only one revision can be active. `research`, `civic`,
`settler`, `city`, and later areas are Strategic Scopes inside that root,
not separate Contract roots.

During migration the active Contract carries a persistent, auditable
Authority Scope Set. Phase 1B persists this structure but requires it to remain
empty, so every Scope is still legacy-owned. After Phase 1C atomically
activates research, the mixed-ownership state is:

```text
research -> MissionGraph authority
civic    -> legacy authority
settler  -> legacy authority
city     -> legacy authority
```

Before deterministic validation accepts a `StrategicContractProposal` or
`MissionGraphPatch`, it derives the writable Scope set from the current
Authority Scope Set. A proposal or patch may add, modify, or delete only
Scopes for which MissionGraph currently holds write authority. A legacy-owned
Scope may be supplied as a read-only game fact or controlled context input,
but it cannot create, modify, or delete an active Mission or change strategic
state for that Scope. An out-of-scope write is rejected deterministically.

The only exception is an atomic Scope authority-switch transaction. That
transaction must update the Authority Scope Set, initialize the newly owned
Scope Missions, record migration audit, stop the legacy authority write path,
and commit one new StrategicContract revision together. No intermediate state
may expose two writers or a MissionGraph Mission for a legacy-owned Scope.

For any scope, legacy authority or MissionGraph authority may write, never
both. Storage may later be physically partitioned, but there remains one
logical root revision, one atomic commit, and one strategic authority.
Scopes cannot commit unrelated Contract roots.

## 5. Target Data Flow

```text
GamePort
   |
Raw RuntimeSnapshot
   |
ObservationNormalizer
   |
Canonical NormalizedObservation
   |
StateDeltaBuilder
   |
MissionImpactAnalyzer
   |
Affected Mission Set
   |
StrategicPlannerPort
   |-- CreateStrategicContract
   `-- RepairMissionGraph
   |
Validated StrategicContractProposal / MissionGraphPatch
   |
WorkflowStateStorePort
   |
Atomic StrategicContract revision commit
   |
TurnCompiler
   |
TurnActionGraph
   |
BatchExecutor
   |
ActionAttempt
   |
GamePort
   |
Fresh Raw RuntimeSnapshot
   |
ObservationNormalizer
   |
Fresh Canonical NormalizedObservation
   |
Verification
```

Canonical NormalizedObservation is the only current game-fact authority.
Compatibility values such as `NormalizedRuntimeObservation.snapshot` and
other RuntimeSnapshot projections may serve unmigrated code only.
MissionGraph, StateDelta, and TurnActionGraph must not consume compatibility
snapshots as facts. Migration cannot leave old and new representations both
treated as current truth.

## 6. StrategicContract and MissionGraph

### 6.1 StrategicContract

The Contract concept expresses at least:

- stable Contract identity and game session;
- active revision and Authority Scope Set;
- strategic objectives and global constraints;
- MissionGraph;
- the Observation evidence from which it was created;
- review, completion, and invalidation conditions;
- approval state;
- a versioned policy snapshot.

Phase 1B persists the minimum Contract identity, revision, objectives,
constraints, approval snapshot, policy snapshot, Authority Scope Set, and
MissionGraph shapes needed for the foundation. The exact model may evolve only
through versioned contracts that preserve one aggregate root and immutable
revision history.

### 6.2 MissionGraph

MissionGraph is internal Contract structure, not a second state source. A
Mission concept expresses at least:

- stable semantic identity and node revision;
- Strategic Scope;
- objective, subjects, and slots;
- dependencies and desired outcomes;
- preconditions, completion conditions, and invalidation conditions;
- review triggers and status;
- evidence references.

Commit invariants:

- one game session has one active Contract revision;
- MissionGraph commits only with its Contract revision;
- every patch declares a base Contract revision;
- a stale patch cannot overwrite a newer revision;
- Mission identity cannot depend on a model-generated random identifier;
- repeating the same patch commit cannot increment revision twice;
- Contract, MissionGraph, patch audit, and Authority Scope Set changes share
  one transaction boundary.

### 6.3 Strategic Proposal Decision and Atomic Scope Activation

Phase 1B persists a StrategicResearchProposal as immutable candidate content.
It is bound to its source PlannerRequest, final ProviderAttempt, target
Contract identity, expected base revision, source Observation, canonical hash,
and Proposal Ready Tick. The explicit-only Human Wait protects that candidate
but does not approve, reject, invalidate, or apply it.

Five facts have distinct authorities:

| Fact | Sole authority |
| --- | --- |
| AI candidate content | StrategicResearchProposal |
| Human APPROVED or REJECTED disposition | ApprovalRecord |
| Permanent invalidation caused by system facts | StrategicProposalInvalidatedTick |
| Effective strategic objectives and Missions | Active StrategicContract revision |
| Current write owner for each Strategic Scope | AuthorityScopeSet in the active revision |

Proposal content remains immutable. Its disposition is derived from durable
facts:

```text
no ApprovalRecord and no Invalidated Tick -> OPEN
APPROVED ApprovalRecord                 -> APPROVED
REJECTED ApprovalRecord                 -> REJECTED
StrategicProposalInvalidatedTick        -> INVALIDATED
```

The StrategicResearchProposal decision protocol permits only APPROVED and
REJECTED human decisions, even when shared approval infrastructure supports
other decisions for other workflows. Each Proposal has at most one human
terminal ApprovalRecord or one system Invalidated Tick. APPROVED plus
REJECTED, either human result plus INVALIDATED, or multiple Invalidated Ticks
are invalid.

Complete durable evidence is:

```text
APPROVED
= Proposal + APPROVED ApprovalRecord + StrategicContractCommit
+ StrategicContract revision + StrategicProposalAppliedTick

REJECTED
= Proposal + REJECTED ApprovalRecord + StrategicProposalRejectedTick

INVALIDATED
= Proposal + StrategicProposalInvalidatedTick
```

A Proposal-derived Contract revision records `approval_status=APPROVED` as
an immutable redundant snapshot of its matching ApprovalRecord. The snapshot
is not approval authority. Foundation revisions may use `NOT_REQUIRED`.
Rejected and invalidated Proposals create no Contract revision.

Approval appends exactly `expected_base_revision + 1` and transfers
research write authority to MissionGraph in the same atomic transaction. That
transaction also binds the ContractCommit structurally to Proposal identity
and hash, Approval identity, source PlannerRequest, and expected base; records
the same Mission identity and revision in ContractCommit and Applied Tick;
moves Runtime to routing; and clears the protected wait. The Mission must have
`scope=research` and `status=ACTIVE`, and the Contract revision must contain
that same immutable Mission. Its only executable operation is selected by the
closed mapping `research -> set_research`; `desired_outcome`, tool-name text,
and free JSON cannot select another action. A stale base or target produces
system invalidation without Approval, automatic rebase, or Provider recall. No
intermediate state may expose partial approval, effective research strategy
without matching authority, or dual research writers.

Before writing approval or changing AuthorityScopeSet, the same BEGIN IMMEDIATE
transaction re-reads every legacy research StoredTask, all ActionAttempts,
and pending task confirmation. It performs the durable cutover disposition and
proves execution quiescence while holding the writer lock. Cleanup observed
before lock acquisition is not authority. Switched authority with claimable,
in-flight, verifying, uncertain, or revivable legacy execution is invalid at
ordinary save, startup, and replay.

Phase 1B StrategicProposalWaitResumedTick is interaction evidence only. Before
PR 1C-3 enables Proposal decisions, a writer-locked migration classifies each
historical OPEN Proposal:

| Historical Proposal state | Enablement treatment |
| --- | --- |
| Valid unresolved Proposal-ready wait | Preserve OPEN for APPROVE or REJECT |
| WaitResumedTick and no terminal fact | INVALIDATED with PRE_PHASE1C_WAIT_RELEASED |
| Stale target or expected base | INVALIDATED with the existing stale reason |
| No Proposal | No operation |

The migration is deterministic and idempotent across multiple Proposals. It
writes no ApprovalRecord, Contract revision, ContractCommit, MissionGraph
authority, StoredTask, or Provider call. Startup and replay recognize the
pre-enable shape as migration input, but enabled ordinary work requires that no
OPEN Proposal already has a WaitResumedTick.

Each migration-origin StrategicProposalInvalidatedTick stores typed origin
`PHASE1C_ENABLEMENT_MIGRATION` and binds its ProposalReadyTick, ResumeRequest,
and StrategicProposalWaitResumedTick. Its canonical logical time is exactly one
microsecond after the maximum persisted timestamp among Proposal creation,
source PlannerRequest completion, final ProviderAttempt completion, Ready Tick
completion, ResumeRequest submission, and WaitResumedTick completion. The Tick
uses that value for both `started_at` and `completed_at`. This is a deterministic
causal frontier, not a reconstructed historical user-action time.

Ordinary validation, startup, and replay resolve the same bindings and recompute
the same value. Missing or mismatched sources, any earlier or different Tick
time, or a non-representable successor fails closed. Migration failure leaves
the pre-upgrade database unchanged; a valid migrated database reopens and its
export/import round trip preserves the Tick without regenerating its time.

After enablement, a strategic_contract_proposal_ready wait can leave only
through the dedicated APPROVE or REJECT aggregate transition.
request_human_resume fails closed for that wait and remains available for
supported non-Proposal waits such as strategic_request_terminated.

RuntimeState, Human Wait context, Applied, Rejected, Resume, and Error Ticks
are transition or interaction evidence. None substitutes for ApprovalRecord.
Ordinary work begins only after the dedicated decision or activation Tick
completes.

Proposal application does not directly create StoredTask. Later routing reads
the active Contract and authoritative MissionGraph revision, performs a
separate deterministic projection, and enters the normal Planner lifecycle.
Proposal or transition-Tick identity alone cannot create claimable work.

## 7. Planner Boundary

The logical model boundary is `StrategicPlannerPort` with two request
semantics:

```text
CreateStrategicContract
RepairMissionGraph
```

Whether one model, parent and child models, or different models implement the
port is deferred. No `ParentAgent`, `ChildAgent`, `TopAgent`,
`MissionAgent`, or multi-agent protocol is introduced here.

Planner receives only controlled projections of Canonical
NormalizedObservation, the current Contract revision and Authority Scope Set,
the affected Mission subgraph, action/entity/condition/query contracts,
declarative information-query results, and approval/risk policies.

Planner cannot call GamePort, query WorkflowStateStore, execute actions,
declare action success, mutate the current Contract, return complete database
objects, bypass patch validation, or create StoredTask objects with
independent strategic meaning. Its only outputs are a
`StrategicContractProposal` or `MissionGraphPatch`. Deterministic validation
precedes atomic commit.

### 7.1 PlannerRequest generalization

PlannerRequest currently depends on DecisionGap identity. It must be
generalized before the DecisionGap write path is disabled for research.

Generalized requests can target StrategicContract creation or MissionGraph
repair. During migration, `decision_gap_ids` may remain a compatibility
reference for old requests, but a Mission request must not manufacture a
synthetic DecisionGap. Request identity must be able to refer to Contract ID,
base Contract revision, Strategic Scope, Affected Mission Set, and patch base
revision.

ProviderAttempt and InformationRound remain attached to PlannerRequest.
Before scope authority switches, every active legacy PlannerRequest completes,
is superseded, or follows an explicit migration path. Legacy and Mission
requests cannot simultaneously be planning authority for one scope.

## 8. StateDelta

StateDelta is deterministic and never model-generated. It compares current
Canonical NormalizedObservation with the last successfully accepted and
persisted compatible historical Observation projection. Historical
Observation is comparison evidence, not current truth.

### 8.1 Completeness

```text
unknown is not empty
not loaded is not deleted
```

Entity absence indicates deletion only when the current collection is
explicitly complete. `None`, `NOT_LOADED`, unloaded fields, and entities not
covered by a partial query are unknown. They cannot mean an empty collection,
entity deletion, a cleared slot, or automatic Mission invalidation.

### 8.2 Comparison baseline

An ordinary Delta is valid only when:

- game session IDs match;
- normalization versions are equal or explicitly compatible;
- source versions are comparable;
- history is the last successfully accepted and persisted Observation;
- current fields have sufficient completeness;
- no unhandled turn regression exists.

Otherwise the result is `initial_baseline` or `rebaseline_required`, never a
fabricated field Delta. Rebaselining applies to a new session, missing
history, turn regression, incompatible normalization or source versions,
partial critical data, or unverifiable history.

`initial_baseline` and `rebaseline_required` are Workflow control results.
They are not ordinary StateDelta values and do not automatically accept the
current Observation as a replacement baseline. A Canonical
NormalizedObservation becomes the accepted baseline only after all of these
conditions hold:

- its game session matches the Workflow session;
- its normalization and source versions are accepted for comparison;
- every collection and field required by the intended comparison has the
  required completeness;
- the Observation is successfully persisted and explicitly accepted.

An incomplete Observation cannot overwrite the last accepted baseline,
manufacture deletion, switch Scope authority, delete a Mission by itself, or
by itself globally invalidate the Contract or trigger a full-graph rebuild.
Independent known and comparable fields may still produce a local Delta;
unavailable portions remain unknown. A later complete Observation is compared
with the unchanged accepted baseline, so data that merely reappears is not
misclassified as newly created.

StateDelta can express explicit entity creation/deletion, known field changes,
known-to-known slot changes, turn and strategic-event changes, and
unknown-to-known transitions.

### 8.3 Evidence provenance

StateDelta may reference ActionAttempt verification evidence, but provenance
is retained:

```text
Observation-derived delta = game fact change
ActionAttempt evidence     = workflow audit and verification fact
```

ActionAttempt is never presented as a game Observation.

## 9. Affected Mission Set and Repair

MissionImpactAnalyzer deterministically expands:

```text
StateDelta
-> directly affected Missions
-> downstream dependencies
-> conflicting shared subject/slot Missions
-> ancestor constraints requiring revalidation
```

The affected closure is the default Planner input. Unrelated changes do not
call Planner. Full-graph rebuilding is an explicit escalation only when the
root objective is contradicted, a global constraint changes incompatibly, a
local patch cannot restore consistency, the Contract completes or globally
invalidates, rebaseline proves the graph unverifiable, or a human explicitly
requests full replanning.

Local repair is the default path. Full rebuild is an explicit escalation.

## 10. Patch Commit and Crash Recovery

MissionGraphPatch declares its base Contract revision, has stable request and
response audit identities, passes deterministic validation, commits
atomically, and records the commit result.

When the same response is processed after a crash:

- an already committed revision does not increment again;
- the patch is not applied again;
- Provider is not called again by default;
- recovery uses persisted PlannerRequest audit.

A stale-base patch is rejected or enters an explicit repair flow. It never
overwrites current state.

ActionAttempt verification is first an independent Workflow audit fact. If a
verified result changes a Mission's status, completion or invalidation state,
desired-outcome satisfaction, or MissionGraph-persisted evidence references,
Runtime must execute a deterministic Contract state transition:

```text
expected current Contract revision
-> validate deterministic Mission transition
-> WorkflowStateStorePort atomic commit
-> new StrategicContract revision
```

MissionGraph is never updated in place. The transition is idempotent: recovery
of the same verified evidence commits at most one new revision. Evidence kept
only as an external ActionAttempt audit record, without a committed Contract
transition, cannot make the corresponding Mission completed or advanced.

## 11. TurnActionGraph

TurnActionGraph is a deterministic execution projection of:

```text
current StrategicContract revision
+ current Authority Scope Set
+ current valid Missions
+ current Canonical NormalizedObservation
+ action contracts
+ approval state
```

Each TurnActionGraph is bound to exactly one game session, one turn, one source
Canonical NormalizedObservation, and one source StrategicContract revision. A
turn change, Contract revision change, or source Observation that no longer
applies makes the graph stale. A TurnActionGraph never remains the active
execution graph across turns; future-turn strategic intent remains in
MissionGraph.

It is not strategic state. It belongs to a turn and Observation, references
its source Contract revision, and links nodes to source Missions. Nodes use
stable idempotent identities and only Action Registry actions. Entity types,
arguments, and conditions pass existing contracts.

Before claim, BatchExecutor checks `source_contract_revision`. READY nodes
from a stale graph cannot be sent. The graph invalidates or recompiles after
a Contract revision change. Committing a new Contract revision after a
deterministic Mission transition therefore makes every graph referencing the
old revision stale; its READY nodes cannot be claimed, and the next Tick must
compile a new current execution projection.

### 11.1 Phased execution authority

After Phase 1C research activation and through Phase 2:

```text
MissionGraph                 = research strategic authority
workflow_tasks / StoredTask  = temporary research execution authority
```

StoredTask is deterministically projected from a current Mission revision.
PR 1C-1 adds this optional, all-or-none provenance group to the existing
StoredTask model and workflow_tasks table:

```text
source_contract_id
source_contract_revision
source_mission_id
source_mission_revision
```

Legacy rows migrate with all four columns NULL. Partial groups, cross-game
references, and a Mission that does not belong to the Contract fail ordinary
save, startup, and replay validation. This PR 1C-1 foundation does not alter
routing or task lifecycle behavior and creates no second task model or table.

The concrete PR 1C-1 persistence version is workflow database v11. Proposal
decisions remain external evidence in the existing `approval_records` table,
terminal transitions remain typed records in `workflow_ticks`, and structured
Proposal provenance remains part of the canonical
`StrategicContractCommit`. No mutable Proposal status or parallel terminal
table exists. A single aggregate validator is reused by ordinary persistence,
startup, and replay preflight, including validation before replay deletes
target-game rows.

All existing public Store methods fail closed when asked to save a strategic
Proposal ApprovalRecord, Proposal-derived ContractCommit, or Applied,
Rejected, or Invalidated Tick independently. Thus v11 can migrate, read,
validate, export, and import complete evidence without enabling a production
decision path. Engine, Human Wait, legacy research routing, task claim, retry,
confirmation, and recovery behavior remain the Phase 1B behavior until the
later dormant and enablement PRs.

Before first research cutover, action_type=set_research with null provenance is
a legacy research StoredTask. After cutover, Mission-derived set_research has
all four fields and matches the same-game active Contract and a Mission whose
scope is exactly `research` and status is exactly `ACTIVE`. The only valid
projected action is the registry action `set_research`; Mission desired_outcome
data cannot name or substitute an action, and civic, production, unit, city,
non-ACTIVE, or other-scope Missions cannot produce research work. ContractCommit
and Applied Tick bind the same Mission identity and revision consumed by
routing. Planner cannot create an independently strategic StoredTask, a stale
Contract or Mission makes old work unclaimable, and an action cannot have two
execution authorities.

The approval transaction freezes legacy research execution under the same
writer lock as the Contract and AuthorityScopeSet commit:

| Existing legacy research state | Atomic cutover treatment |
| --- | --- |
| PENDING, READY, BLOCKED, FAILED, ESCALATED | Move to CANCELLED and audit the previous state and authority-switch reason |
| AWAITING_CONFIRMATION | Cancel the task and close the legacy confirmation; never interpret it as Contract approval |
| RUNNING | Block activation until mutation-boundary recovery completes |
| VERIFYING | Block activation until fresh verification completes |
| UNCERTAIN | Block activation until fact-based or human reconciliation completes |
| DONE, CANCELLED, EXPIRED | Preserve as inert history when no unresolved Attempt exists |

Any ActionAttempt in PREPARED, VERIFYING, or UNCERTAIN blocks activation
regardless of task status. CANCELLED is permanently non-revivable for this
cutover. The locked transaction re-reads all affected rows, applies every safe
cancellation, records legacy disposition in the activation audit, and proves no
claimable, in-flight, verifying, uncertain, or revivable research execution
remains before switching authority. A READY task created after preflight but
before lock acquisition is therefore observed and cancelled or blocks the
transaction.

The authority-switch or Proposal-application transaction never creates a
Mission-derived StoredTask. After activation and the dedicated
Decision/Activation Tick complete, a later Routing step reads the active
Contract and Mission, performs a separate deterministic revision-bound
projection, and enters the normal Planner lifecycle before a replacement
StoredTask may be created. Old execution provenance remains auditable and at
most one equivalent action may be claimable.

After cutover, every research claim, retry, confirmation release, and recovery
operation re-reads AuthorityScopeSet and the active Contract/Mission revision
inside its own write transaction. Provenance-free legacy work and complete but
stale provenance cannot become claimable. Every legacy task creation and
equivalent mutation path fails closed.

Phase 3 makes `domain.Task` and TurnActionGraph the research execution
authority. `workflow_tasks` and `models.StoredTask` stop deciding research
actions and remain only for migration or historical reads. Long-term dual
writes are prohibited.

## 12. Wave, Barrier, and BatchExecutor

Batch execution does not mean concurrent game writes. Initial execution
retains at most one mutation per Tick.

A Wave is the candidate set whose dependencies are currently satisfied.
WorkflowRuntime claims at most one node each Tick. Barrier has four kinds:

1. Dependency Barrier
2. Verification Barrier
3. Approval Barrier
4. Turn Barrier

A Turn Barrier excludes a node from the current claimable Wave until its
target turn arrives. On entering a new turn, Runtime reads and normalizes a new
Observation, compiles a new TurnActionGraph, and reevaluates the Turn Barrier
against that graph. It cannot carry a READY node from the previous current-turn
graph across the turn boundary. Approval, verification, and ActionAttempt audit
can recover durably, but execution claim always uses the current graph.

This is not a general workflow language, plugin platform, distributed queue,
or generic DAG executor. BatchExecutor cannot call Planner.

## 13. WorkflowStateStorePort

All workflow state remains behind one `WorkflowStateStorePort`. The design
does not create `StrategicContractRepository`, `MissionRepository`,
`PatchRepository`, or `TurnActionRepository`.

Phase 1B exposes typed Contract and Proposal operations on the one Store Port.
SQLite implements the port while Domain remains independent of SQLite.
Contract, MissionGraph, Proposal decision audit, patch audit, and Authority
Scope Set changes commit through one database authority and transaction.
Phase 1C extends those typed operations and aggregate validators; it does not
create a Proposal, Contract, Mission, or approval Repository beside the Store.

## 14. State Authority Matrix

| Information | Sole authority |
| --- | --- |
| Current game facts | Current Canonical NormalizedObservation |
| Historical game facts | Accepted historical Observation projections |
| AI strategic candidate content | StrategicResearchProposal |
| Human Proposal disposition | ApprovalRecord |
| System Proposal invalidation | StrategicProposalInvalidatedTick |
| Strategic objectives and Missions | Current StrategicContract revision |
| Strategic Scope write ownership | AuthorityScopeSet in the current StrategicContract revision |
| Proposal decision transition audit | Applied, Rejected, Resume, and Error Ticks |
| Current-turn action dependencies | Current TurnActionGraph revision |
| Research action execution through Phase 2 | Deterministically projected StoredTask |
| Phase 3+ research action execution | TurnActionGraph / `domain.Task` |
| Action delivery boundary | ActionAttempt |
| Model call boundary | PlannerRequest / ProviderAttempt |
| Information query continuation | InformationRound |
| Action contracts | Action Registry, Condition Contract, and validation |
| Runtime phase | Workflow State in WorkflowStateStore |
| Configuration policy | Loaded and versioned configuration snapshot |

Model output is a proposal. Cache is not a fact source. Events are not current
game facts. Compatibility RuntimeSnapshot is not a MissionGraph fact source.
StrategicProposalInvalidatedTick also records an invalidation transition, but
its core identity remains the sole authority for the system terminal outcome.
PlanLease and MissionGraph cannot both decide one Strategic Scope. StoredTask
and TurnActionGraph cannot both decide one action.

## 15. Module Boundaries and Dependencies

Conceptual modules, not files created by PR 0:

```text
domain/
  strategic_contract
  mission_graph
  state_delta
  turn_action_graph
  execution

application/
  state_delta_builder
  mission_impact_analyzer
  mission_repair_service
  turn_compiler
  batch_executor
  workflow_runtime

ports/
  game_port
  strategic_planner_port
  workflow_state_store_port

adapters/
  sqlite
  civ6_mcp
  planner_provider
  replay
  web
```

Dependency graph:

```text
Bootstrap
|---> Application
`---> Adapters

Application
|---> Domain
`---> Ports

Adapters
`---> Ports

Ports
`---> Domain contracts

Domain
`---> no outer layer
```

Domain does not depend on Application, Ports, or Adapters. Application does
not depend on concrete adapters. Adapters implement Ports. Bootstrap alone
instantiates and connects the graph.

Forbidden dependencies include Domain to SQLite, GamePort, or Planner
provider; Planner to WorkflowStateStore or GamePort; BatchExecutor to Planner;
WorkflowRuntime to SQLite SQL or concrete MCP methods.

## 16. Runtime Sequences

### 16.1 New game

```text
Raw RuntimeSnapshot
-> ObservationNormalizer
-> Canonical NormalizedObservation
-> initial baseline
-> CreateStrategicContract request
-> validate proposal
-> atomic Contract revision 1
-> compile current execution projection
```

### 16.2 Ordinary state change

```text
Canonical NormalizedObservation
-> StateDelta
-> Affected Mission Set
-> no impact: continue
-> impact: MissionGraphPatch
-> validate
-> atomic new Contract revision
-> invalidate/recompile affected execution projection
```

### 16.3 Incomplete Observation

```text
units not loaded / slot NOT_LOADED
-> unknown
-> no deletion Delta
-> collect allowed read-only information when needed
   or wait for a complete Observation
```

### 16.4 Normalization version change

```text
incompatible historical and current normalization versions
-> rebaseline_required
-> no ordinary field Delta
-> no automatic full-graph invalidation
```

### 16.5 Action execution

```text
select one node from current Wave
-> check Contract revision
-> check Barrier and preconditions
-> persist PREPARED ActionAttempt
-> send one mutation
-> obtain fresh Raw RuntimeSnapshot
-> ObservationNormalizer
-> fresh Canonical NormalizedObservation
-> verification
-> persist independent ActionAttempt verification audit
-> if Mission state or persisted evidence changes, commit one deterministic
   Contract transition and new StrategicContract revision
-> invalidate the old TurnActionGraph and recompile on the next Tick
```

### 16.6 Insufficient Planner information

```text
Planner returns InformationRequest
-> Query Service executes an allowed read-only tool
-> persist InformationRound
-> continue the same PlannerRequest
```

### 16.7 Patch crash recovery

```text
Planner response persisted
-> crash during commit
-> restart reads PlannerRequest audit
-> check whether Patch already committed
-> committed: recover result
-> not committed: commit or reject against same base revision
-> do not call Provider again by default
```

### 16.8 Human approval

```text
Patch or action node requires approval
-> persist pending approval
-> stop mutation
-> persist ApprovalRecord
-> next Tick revalidates against fresh Observation
```

StrategicResearchProposal uses the stricter atomic sequence:

```text
Proposal Ready + explicit-only Human Wait
-> user APPROVED / REJECTED or system detects stale Proposal
-> dedicated decision transaction and transition Tick
-> APPROVED: Contract revision and research authority commit together
-> REJECTED / INVALIDATED: no Contract revision
-> transition Tick completes
-> Runtime resumes routing
```

## 17. Preserved Safety Semantics

Migration preserves:

- one bootstrap composition root;
- at most one mutation per Tick;
- ActionAttempt persistence before delivery;
- no blind retry after UNKNOWN;
- fresh Canonical NormalizedObservation verification;
- ProviderAttempt and versioned, deduplicated PlannerRequest;
- InformationRound and approvals;
- replay and Windows/Linux consistency;
- cross-platform Tick process lock;
- database transactions and recovery;
- Action Registry as action fact source;
- Planner input-contract projection.

## 18. Non-goals and Deferred Decisions

This architecture excludes Proposal editing, automatic approval or rebase,
approval permissions, multi-approver workflows, revocation of effective
Contract revisions, multiple MissionGraph roots, non-research authority
activation during Phase 1C, generic MissionGraph editing, parent/child Agent
protocols, multi-agent platforms, graph databases, CQRS, generic event
sourcing, distributed queues, multiple mutations per Tick, generic Barrier
plugins, UI redesign, and a complete Civ6 tool list.

A migration phase introduces only the concrete boundaries it needs. It does
not pre-create empty interfaces or parallel runtime, Store, approval,
planning, or execution implementations.
