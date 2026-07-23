# ADR 0002: One StrategicContract Aggregate per Game Session

- Status: Proposed
- Date: 2026-07-23
- Scope: strategic aggregate identity, scope authority, MissionGraph repair

Related documents:

- [ADR 0001: Explicit Runtime Composition](0001-explicit-runtime-composition.md)
- [MissionGraph Runtime SDD](../architecture/mission-graph-sdd.md)
- [MissionGraph Runtime Migration Plan](../plans/2026-07-23-mission-graph-migration.md)

## Context

The current runtime persists strategic intent through DecisionGap, Plan,
PlanLease, PlanBundle, and StoredTask. Migrating each strategic area to an
independent new root would exchange one fragmented authority model for
another. It would also make cross-scope constraints and atomic migration
authority difficult to audit.

The target needs local Mission repair without allowing research, civic,
settler, and city planning to become unrelated strategic roots.

## Decision

1. Each game session has exactly one active StrategicContract revision.
2. Strategic Scope is an internal authority partition of that Contract.
3. MissionGraph is structure inside the Contract aggregate, not a separate
   state authority.
4. The Contract has a persistent, auditable Authority Scope Set for migration.
5. Each Strategic Scope has either legacy or MissionGraph write authority,
   never both.
6. Local MissionGraphPatch is the default repair operation.
7. Every patch declares its base Contract revision.
8. Reprocessing the same accepted patch is idempotent and does not add another
   Contract revision.
9. Full-graph rebuilding is an explicit escalation for root or global
   inconsistency, not the ordinary response to state change.
10. Planner output is a proposal until deterministic validation and atomic
    Contract commit succeed.
11. A `StrategicContractProposal` or `MissionGraphPatch` may add, modify, or
    delete only Scopes in the current Authority Scope Set for which
    MissionGraph has write authority. Legacy-owned Scopes are read-only facts
    or controlled context and any attempted strategic write is rejected
    deterministically.
12. The only way to initialize Missions for a legacy-owned Scope is an atomic
    Scope authority-switch transaction that updates the Authority Scope Set,
    initializes the new Scope Missions, records migration audit, stops the
    legacy write path, and commits one new StrategicContract revision.
13. ActionAttempt verification remains an independent Workflow audit fact. If
    verification changes Mission status, completion or invalidation,
    desired-outcome satisfaction, or MissionGraph-persisted evidence, it must
    perform an idempotent deterministic Contract transition from the expected
    current revision through WorkflowStateStorePort atomic commit to exactly
    one new StrategicContract revision. MissionGraph is never updated in place.
14. External ActionAttempt evidence that is not committed through such a
    Contract transition cannot make a Mission completed or advanced.

The physical representation may later be partitioned, but Contract revision,
MissionGraph, patch audit, and Authority Scope Set changes share one logical
aggregate and transaction boundary.

## Consequences

### Positive

- Cross-scope constraints have one strategic root.
- Scope-by-scope migration remains explicit and auditable.
- Optimistic revision checks prevent stale patches from overwriting newer
  strategy.
- Local repair limits model input and avoids unrelated replanning.
- Legacy and MissionGraph ownership cannot silently overlap.
- Mission progress and evidence have the same revision and recovery guarantees
  as every other MissionGraph mutation.

### Negative

- Contract commits must coordinate all changed aggregate parts atomically.
- Scope migrations require an explicit audit of active legacy objects.
- Stable Mission identity and patch idempotency require deterministic rules.
- Large root-level changes may still require explicit full-graph rebuilding.

## Rejected Alternatives

### One independent Contract per Strategic Scope

Rejected because scopes would become unrelated roots and global constraints
would have no single revision or atomic authority.

### Contract and MissionGraph as separate authorities

Rejected because their revisions could diverge and recovery could observe an
invalid combination.

### One Agent per Mission

Rejected because model topology is an adapter decision, not domain identity,
and would create an unnecessary agent framework.

### Add MissionGraph on top of PlanLease

Rejected because both could decide the same scope and preserve two strategic
authorities indefinitely.

### Keep the graph only in memory

Rejected because crash recovery and audit require durable workflow state.

### Use a graph database

Rejected because the current SQLite authority can persist the needed
aggregate and transactional semantics.

### Rewrite the complete plan after every change

Rejected because unrelated changes must not trigger Planner calls and local
patching is the default.

## Follow-up Constraint

Implementation begins with PlannerRequest generalization, then migrates one
Strategic Scope under the Authority Scope Set. It does not create a second
Engine or a third long-lived Plan model.
