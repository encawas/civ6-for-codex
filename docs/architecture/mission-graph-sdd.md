# MissionGraph Runtime Software Design Description

Status: Proposed
Date: 2026-07-23

Related documents:

- [ADR 0001: Explicit Runtime Composition](../adr/0001-explicit-runtime-composition.md)
- [ADR 0002: One StrategicContract Aggregate per Game Session](../adr/0002-strategic-contract-mission-graph.md)
- [ADR 0003: State Authority, Observation Completeness, and Module Boundaries](../adr/0003-state-authority-module-boundaries.md)
- [ADR 0004: TurnActionGraph and Wave/Barrier Execution](../adr/0004-turn-action-graph-execution.md)
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
| `PlannerRequest` | Durable logical planning request | Generalize and retain |
| `ProviderAttempt` | Audit of calls across the model boundary | Retain |
| `InformationRound` | Declarative information-gathering continuation | Retain |
| `Plan` / `PlanLease` | Current durable intent and validity | Replace by scope with StrategicContract/MissionGraph |
| `models.PlanBundle` | Legacy Planner output and task collection | Delete after migration adapters retire |
| `models.StoredTask` | Current production execution task | Retain as execution authority in Phases 1-2; retire in Phase 3 |
| `domain.Task` | New domain task model | Evolve into the canonical TurnActionGraph node contract |
| `ActionAttempt` | Action delivery, recovery, and verification audit | Retain |
| Rules / Progression | Compile deterministic tasks and events | Retain; never make them a new state authority |
| `strategy_state` and legacy plan tables | Legacy planning state | Stop writes by scope, migrate data, then delete |
| `workflow_tasks` | Legacy execution task table | Retire as execution authority after Phase 3 |
| `workflow_ticks` | Tick audit | Retain |

The repository currently contains legacy types in `models.py` and newer types
in `domain/`. MissionGraph migration must converge those types. It must not
add a third long-lived Plan, Task, or Runtime state model.

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
Authority Scope Set. Physical fields and tables are deferred, but its meaning
is fixed:

```text
research -> MissionGraph authority
civic    -> legacy authority
settler  -> legacy authority
city     -> legacy authority
```

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

PR 0 intentionally does not fix exact fields, validation models, or database
columns.

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

It is not strategic state. It belongs to a turn and Observation, references
its source Contract revision, and links nodes to source Missions. Nodes use
stable idempotent identities and only Action Registry actions. Entity types,
arguments, and conditions pass existing contracts.

Before claim, BatchExecutor checks `source_contract_revision`. READY nodes
from a stale graph cannot be sent. The graph invalidates or recompiles after
a Contract revision change.

### 11.1 Phased execution authority

Phases 1-2:

```text
MissionGraph                 = research strategic authority
workflow_tasks / StoredTask  = temporary research execution authority
```

StoredTask is deterministically projected from a current Mission revision and
references its Mission and Contract revision. Planner cannot create an
independently strategic StoredTask. A stale Contract makes old StoredTask
unclaimable, and an action cannot have two execution authorities.

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

This is not a general workflow language, plugin platform, distributed queue,
or generic DAG executor. BatchExecutor cannot call Planner.

## 13. WorkflowStateStorePort

All workflow state remains behind one `WorkflowStateStorePort`. The design
does not create `StrategicContractRepository`, `MissionRepository`,
`PatchRepository`, or `TurnActionRepository`.

Typed Contract methods may be added to the one Store Port. SQLite implements
the port while Domain remains independent of SQLite. Contract, MissionGraph,
patch audit, and Authority Scope Set changes commit through one database
authority and transaction. The current dynamic `__getattr__` boundary should
later converge to explicit typed methods, but PR 0 does not implement it.

## 14. State Authority Matrix

| Information | Sole authority |
| --- | --- |
| Current game facts | Current Canonical NormalizedObservation |
| Historical game facts | Accepted historical Observation projections |
| Strategic objectives, Missions, and scope authority | Current StrategicContract revision |
| Current-turn action dependencies | Current TurnActionGraph revision |
| Phase 1-2 research action execution | Deterministically projected StoredTask |
| Phase 3+ research action execution | TurnActionGraph / `domain.Task` |
| Action delivery boundary | ActionAttempt |
| Model call boundary | PlannerRequest / ProviderAttempt |
| Information query continuation | InformationRound |
| Approval facts | ApprovalRecord |
| Action contracts | Action Registry, Condition Contract, and validation |
| Runtime phase | Workflow State in WorkflowStateStore |
| Configuration policy | Loaded and versioned configuration snapshot |

Model output is a proposal. Cache is not a fact source. Events are not current
game facts. Compatibility RuntimeSnapshot is not a MissionGraph fact source.
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
-> update execution node and Mission evidence
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

PR 0 excludes functional code, database fields/indexes, Patch JSON Schema,
exact Pydantic fields, model selection, parent/child model protocols,
multi-agent platforms, graph databases, CQRS, generic event sourcing,
distributed queues, multiple mutations per Tick, generic Barrier plugins,
new city-production semantics, UI redesign, and a complete Civ6 tool list.

It also does not create conceptual module files or pre-create empty
interfaces. A migration phase introduces only the boundaries it needs.
