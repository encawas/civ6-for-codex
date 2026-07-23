# ADR 0003: State Authority, Observation Completeness, and Module Boundaries

- Status: Proposed
- Date: 2026-07-23
- Scope: game facts, workflow facts, ports, dependency direction

Related documents:

- [ADR 0001: Explicit Runtime Composition](0001-explicit-runtime-composition.md)
- [MissionGraph Runtime SDD](../architecture/mission-graph-sdd.md)
- [MissionGraph Runtime Migration Plan](../plans/2026-07-23-mission-graph-migration.md)

## Context

The current runtime includes a raw RuntimeSnapshot, a canonical normalized
view, compatibility snapshot access, events, durable workflow records, and
SQLite state. Without explicit authority boundaries, new MissionGraph code
could treat a partial or compatibility projection as current truth, while
runtime, model, and adapter layers could each acquire direct state access.

Migration also needs scope-by-scope ownership without production dual writes.

## Decision

1. GamePort returns a raw RuntimeSnapshot, which passes through
   ObservationNormalizer before new architecture code reads it.
2. Canonical NormalizedObservation is the sole authority for current game
   facts.
3. Compatibility RuntimeSnapshot projections are available only to
   unmigrated code and are not MissionGraph inputs.
4. Unknown or not-loaded data is not empty or deleted. Entity absence means
   deletion only in an explicitly complete current collection.
5. Incompatible normalization/source versions, missing history, turn
   regression, or insufficient completeness produce `initial_baseline` or
   `rebaseline_required`, not an ordinary StateDelta.
6. WorkflowStateStore is the sole workflow-state authority.
7. Planner receives controlled inputs and accesses neither Store nor GamePort.
8. BatchExecutor executes deterministic actions and never calls Planner.
9. WorkflowRuntime orchestrates application services and contains no SQL or
   concrete MCP calls.
10. Bootstrap remains the only production composition root.
11. Each Strategic Scope has one write authority; legacy and MissionGraph
    writes never overlap.
    The persistent Authority Scope Set records that ownership and each switch.
12. Dependencies follow the branching graph in the SDD rather than a false
    single linear chain.
13. StrategicContract, Mission, patch, and TurnAction state use typed methods
    on one WorkflowStateStorePort; no type-specific Repository family is
    created.

`initial_baseline` and `rebaseline_required` are Workflow control results, not
ordinary StateDelta values and not permission to accept the current
Observation automatically. A Canonical NormalizedObservation replaces the
accepted baseline only when its session is consistent, its normalization and
source versions are acceptable, all collections and fields required for the
comparison have sufficient completeness, and persistence plus explicit
acceptance both succeed.

An incomplete Observation does not overwrite the last accepted baseline,
manufacture deletion, switch Scope authority, delete a Mission by itself, or
by itself globally invalidate the Contract or rebuild the full MissionGraph.
Independent fields that are known and comparable may still produce local
Delta; unavailable portions remain unknown. The next complete Observation is
compared against the unchanged accepted baseline, preventing reappearing data
from being classified as newly created.

ActionAttempt evidence remains workflow verification evidence. It may be
referenced by StateDelta but is not disguised as an Observation. Events and
caches likewise do not replace current Canonical NormalizedObservation.

## Dependency Direction

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

Domain has no dependency on Application, Ports, SQLite, MCP, or model
providers. Application depends on abstractions, not concrete adapters.
Adapters implement Ports. Bootstrap selects implementations.

## Consequences

### Positive

- A single representation answers questions about current game facts.
- Partial reads cannot manufacture deletion or invalidation.
- StateDelta has explicit baseline and provenance rules.
- An incomplete read cannot silently replace an accepted comparison baseline.
- Planner and Executor remain independently testable and auditable.
- SQLite remains replaceable behind the Store Port without fragmenting state.
- Scope authority can switch atomically without production dual writes.

### Negative

- Compatibility snapshot consumers must be retired deliberately.
- Observation completeness and version compatibility need explicit metadata.
- Store Port methods must become typed as new aggregate operations arrive.
- Runtime logic that currently reaches concrete details must move behind
  application services or ports.

## Rejected Alternatives

- **Planner calls MCP directly:** mixes proposal and game-side effects.
- **Planner queries SQLite directly:** bypasses controlled inputs and state
  authority.
- **WorkflowRuntime contains SQL:** binds orchestration to one adapter.
- **Compatibility snapshot and canonical Observation are co-authoritative:**
  permits contradictory game facts.
- **Event log replaces current Observation:** events are evidence, not a
  complete current state.
- **Write legacy Plan and MissionGraph together:** creates two authorities for
  one Strategic Scope.
- **Create a second Engine:** makes migration a permanent runtime fork.
- **Import order selects implementation:** violates explicit composition from
  ADR 0001.
- **Repository per new type:** fragments transactions and creates unnecessary
  abstraction.

## Follow-up Constraint

Migration introduces only the typed Store methods and ports required by each
phase. PR 0 does not create empty interfaces or reorganize source files.
