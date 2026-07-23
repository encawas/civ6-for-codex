# ADR 0004: TurnActionGraph and Wave/Barrier Execution

- Status: Proposed
- Date: 2026-07-23
- Scope: deterministic action projection, execution ordering, verification

Related documents:

- [ADR 0001: Explicit Runtime Composition](0001-explicit-runtime-composition.md)
- [MissionGraph Runtime SDD](../architecture/mission-graph-sdd.md)
- [MissionGraph Runtime Migration Plan](../plans/2026-07-23-mission-graph-migration.md)

## Context

StrategicContract and MissionGraph describe durable intent, but they must not
become an imperative action runner. The runtime needs a deterministic,
revision-bound projection that orders current-turn actions while preserving
the proven one-mutation Tick, durable ActionAttempt, and fresh-observation
verification boundaries.

The word "batch" must not imply parallel game writes or execution of an entire
graph in one Tick.

## Decision

1. TurnActionGraph is a deterministic current-turn execution projection, not
   strategic state.
2. Each graph identifies its source Contract revision and source Canonical
   NormalizedObservation.
3. A stale graph cannot execute; source Contract revision is checked again
   before a node is claimed.
4. BatchExecutor executes only validated deterministic Action Registry
   actions and never calls Planner.
5. Runtime sends at most one mutation per Tick in the initial architecture.
6. Wave is a candidate set with satisfied dependencies, not a parallel send.
7. Barrier has exactly four forms: Dependency, Verification, Approval, and
   Turn.
8. Action delivery retains PREPARED ActionAttempt persistence and verification
   against a fresh Canonical NormalizedObservation.
9. In Phases 1-2, deterministically projected StoredTask remains the temporary
   execution authority for migrated research actions.
10. In Phase 3, `domain.Task` and TurnActionGraph become research execution
    authority.
11. StoredTask and TurnActionNode cannot both decide the same action.
12. TurnActionGraph and BatchExecutor are not a generic DAG or workflow
    platform.
13. Each TurnActionGraph binds exactly one game session, one turn, one source
    Canonical NormalizedObservation, and one source Contract revision.
14. A turn change, Contract revision change, or inapplicable source
    Observation makes the graph stale. The graph cannot remain active across
    turns; future-turn intent remains in MissionGraph.
15. A Turn Barrier keeps a future-turn node out of the current claimable Wave.
    Entering a new turn requires a new normalized Observation and a newly
    compiled TurnActionGraph before the Barrier is reevaluated.
16. Approval, verification, and ActionAttempt audit may recover durably, but a
    node claim always comes from the current graph.

WorkflowRuntime remains the sole orchestrator that selects when compilation
or execution advances; it does not absorb BatchExecutor behavior.

Each TurnActionNode has stable idempotent identity, references its source
Mission and Contract revision, and satisfies existing action, entity,
argument, condition, risk, and approval contracts.

## Execution Semantics

```text
compile current Contract and Observation
-> select current Wave
-> choose at most one eligible node
-> verify source Contract revision
-> evaluate Barrier and preconditions
-> persist PREPARED ActionAttempt
-> send one mutation
-> read and normalize a fresh Observation
-> verify postconditions
-> persist independent ActionAttempt verification audit
-> when Mission state or persisted evidence changes, validate and atomically
   commit one deterministic Contract transition as a new Contract revision
-> make the old TurnActionGraph stale and recompile on the next Tick
```

UNKNOWN delivery or verification does not cause blind resend. Approval and
verification barriers are durable and survive restart.

The deterministic Contract transition starts from the expected current
revision and is idempotent: duplicate recovery of the same verification does
not increment revision again. MissionGraph is never updated in place. Once the
new revision commits, old READY nodes cannot be claimed. ActionAttempt evidence
kept only as an external audit record cannot advance or complete a Mission.

When turn N+1 begins, Runtime cannot retain claimable READY nodes from turn N.
It first reads and normalizes the new Observation, compiles the turn N+1 graph,
and only then evaluates Turn Barriers and selects a Wave.

## Consequences

### Positive

- Strategic intent is separated from deterministic action execution.
- Stale Contract revisions cannot leak READY actions into the game.
- Existing delivery and verification safety semantics remain usable.
- Waves expose available work without adding parallel writes.
- The execution model can replace StoredTask incrementally.
- Turn boundaries cannot leak stale READY work into a later game turn.

### Negative

- A Contract revision change may invalidate and recompile execution work.
- Stable node identity and provenance become mandatory.
- Phase 1-2 require a temporary projection into StoredTask.
- Throughput remains deliberately bounded to one mutation per Tick.

## Rejected Alternatives

- **Executor calls Planner on demand:** couples deterministic execution to
  model availability and bypasses patch validation.
- **Execute the whole graph at once:** violates mutation and verification
  boundaries.
- **Continue without fresh verification:** can act on stale game facts.
- **Multiple mutations per Tick:** expands risk before the new execution model
  is proven.
- **Dual-write StoredTask and TurnActionNode as authorities:** permits duplicate
  action delivery.
- **Generic Barrier plugins:** adds a workflow language beyond current needs.
- **Celery, Kafka, or distributed scheduling:** unnecessary for the local
  SQLite and game-session runtime.

## Follow-up Constraint

BatchExecutor is extracted from the existing Engine only after TurnActionGraph
becomes the relevant execution authority. It reuses BoundedGamePort and
ActionAttempt rather than replacing their safety semantics.
