# Refactor Execution Plan

## 1. Strategy

This refactor MUST use incremental replacement, not a big-bang rewrite.

The current repository already contains working behavior, safety fixes, real-game findings, and regression tests. The goal is to preserve that evidence while replacing the layered runtime with explicit domain contracts and a bounded state machine.

The migration pattern is:

```text
characterize current behavior
→ introduce canonical boundary
→ route one behavior through it
→ compare old and new results
→ cut over
→ remove the old path
```

A phase is not complete while two implementations remain silently authoritative.

## 2. Target module boundaries

The exact filenames MAY change, but the final dependency direction SHOULD resemble:

```text
src/civ6_workflow/
├─ domain/
│  ├─ observations.py
│  ├─ events.py
│  ├─ decisions.py
│  ├─ plans.py
│  ├─ tasks.py
│  ├─ attempts.py
│  └─ conditions.py
├─ application/
│  ├─ tick_runner.py
│  ├─ state_machine.py
│  ├─ reconciler.py
│  ├─ router.py
│  ├─ task_materializer.py
│  ├─ planner_policy.py
│  └─ end_turn_policy.py
├─ ports/
│  ├─ game.py
│  ├─ planner.py
│  ├─ store.py
│  └─ clock.py
├─ adapters/
│  ├─ civ6_mcp/
│  ├─ responses_planner/
│  ├─ codex_cli_planner/
│  ├─ sqlite/
│  └─ web/
└─ bootstrap.py
```

Domain and application code MUST NOT import concrete MCP, HTTP, SQLite, or web implementations.

## 3. Phase 0 — freeze the behavioral baseline

### Deliverables

- Inventory public entry points, configuration fields, database schema, action types, condition types, and frontend endpoints.
- Identify every import-time patch or `safe_*` replacement and its reason.
- Add characterization tests for current verified behavior before moving code.
- Capture one or more replay fixtures from real or representative game states.
- Record baseline metrics: tick duration, planner calls per turn, action attempts, retries, and failure classifications.

### Mandatory characterization cases

- zero-city settler flow;
- empty production normalization including `nothing`;
- active research preventing queued replacement;
- event disappearance and reconciliation;
- irreversible action transport failure;
- later observation proving an uncertain action succeeded;
- end-turn tool success without turn-number increase;
- planner timeout/backoff;
- duplicate tick invocation;
- runtime restart with pending durable state.

### Exit gate

No architecture change begins until the critical existing behaviors can be executed through tests or replay fixtures.

## 4. Phase 1 — introduce canonical domain types

### Deliverables

- Add immutable/versioned types for observation, event, decision gap, plan, task, action attempt, approval, and planner request.
- Add explicit canonical statuses rather than reusing one generic status.
- Add observation revision, plan revision, task idempotency key, and slot identity.
- Add translation adapters from existing models/database rows into canonical types.

### Constraints

- Do not change game behavior yet.
- Existing modules MAY continue operating behind adapters.
- New domain types MUST not import existing engine, planner, MCP, or web modules.

### Tests

- normalization property tests;
- serialization round trips;
- invalid state combination rejection;
- idempotency-key stability;
- slot conflict detection;
- schema-version parsing.

### Exit gate

All newly written runtime logic uses canonical types; legacy types are confined to compatibility adapters.

## 5. Phase 2 — create the bounded tick state machine

### Deliverables

- Introduce explicit runtime states from `RUNTIME_STATE_MACHINE.md`.
- Add `WorkflowTick` and a closed `TickOutcome` type.
- Add a structural per-tick mutation budget of one.
- Separate continuous mode from tick execution.
- Make waiting and failure states terminate the current tick.

### Migration order

1. observation;
2. reconciliation;
3. deterministic routing;
4. one-task execution;
5. verification;
6. end-turn transition;
7. planner transitions.

### Constraints

- Do not place a compatibility call to the old full-turn loop inside the new state machine.
- A tick may call legacy helpers, but only one explicit state transition owns each helper.
- No planner call and game mutation may occur in the same tick after mutation delivery.

### Tests

- recording game port fails on second mutation;
- each terminal state ends the tick;
- duplicate tick calls remain idempotent;
- state survives restart between every transition;
- continuous runner only schedules bounded ticks.

### Exit gate

All runtime entry points execute through the canonical bounded tick runner.

## 6. Phase 3 — separate mutation attempt from verification

### Deliverables

- Persist `PREPARED` attempt before external delivery.
- Convert tool success into `VERIFYING`, not task success.
- Move verification to a later fresh observation.
- Implement per-action retry classification.
- Implement automatic reconciliation from later evidence.
- Treat `end_turn` as a normal protected mutation with specialized postcondition.

### Priority action contracts

1. city production;
2. research;
3. civic;
4. unit movement;
5. city founding;
6. builder improvement;
7. end turn;
8. purchases/diplomacy only after dedicated contracts.

### Exit gate

No mutating action is marked successful solely from an MCP/tool return value.

## 7. Phase 4 — plan leases and planner-call policy

### Deliverables

- Add decision gaps as first-class records.
- Add a planner eligibility gate.
- Add one logical request per turn default budget.
- Distinguish logical request from provider retry attempts.
- Add focused context projections and hashes.
- Add decision batching.
- Add plan leases, scope-specific invalidation, cooldown, and review gates.

### Required behavior

- stable ordinary turns call no planner;
- a valid research/production/unit plan suppresses replanning;
- routine unit orders never trigger a planner call;
- unchanged decision input deduplicates repeated requests;
- planner failure does not stop unrelated deterministic work;
- partial plan invalidation creates only bounded new gaps.

### Exit gate

Replay of a stable multi-turn opening demonstrates the planner-call target without weakening safety checks.

## 8. Phase 5 — canonical persistence and migrations

### Deliverables

- Versioned schema for canonical records.
- Forward migration from the current SQLite state.
- Explicit compatibility period and removal date/phase.
- Revision/CAS updates for mutable workflow state.
- Game-session lease with fencing semantics.
- Replay and audit queries using canonical records.

### Constraints

- Avoid permanent dual writes.
- If dual writes are temporarily necessary, compare results and fail visibly on divergence.
- Do not delete old data until migration and rollback evidence is complete.

### Exit gate

A copied real state database migrates, resumes safely, and passes restart scenarios.

## 9. Phase 6 — remove patch layers

### Deliverables

- Replace import-time class substitution with normal dependency injection/bootstrap wiring.
- Move retained logic from `safe_*`, overlays, and compatibility modules into canonical modules.
- Delete obsolete implementations and imports.
- Ensure one canonical registry for actions and conditions.
- Ensure one canonical engine/state machine.

### Removal rule

A shadow module may be deleted only after:

- all callers use the canonical boundary;
- equivalent tests pass against the canonical implementation;
- configuration and migration paths are updated;
- no runtime import depends on replacement order.

### Exit gate

Importing the package does not mutate module classes or registries as a hidden side effect.

## 10. Phase 7 — frontend and operational controls

### Deliverables

Expose at least:

- current runtime state;
- current observation turn/revision;
- last verified mutation;
- unresolved/uncertain attempt;
- current plan leases and invalidation reason;
- decision gaps and routes;
- logical planner calls this turn;
- pending approval with approve/reject/edit/replan;
- pause/resume/manual takeover;
- turn-transition status;
- separate latency/error categories.

The frontend MUST not infer workflow state from logs alone.

## 11. Phase 8 — long-run validation

### Test matrix

- read-only mode;
- approval mode;
- supervised auto mode;
- process kill before send, during send, after response, and before verification;
- FireTuner disconnect/reconnect;
- reload the same save;
- load a different save;
- planner authentication, rate limit, timeout, malformed output, and outage;
- duplicate HTTP requests to tick/approve endpoints;
- multiple runtime processes;
- 20+ turn replay and live smoke test.

### Performance measures

Track:

- median and p95 tick latency by operation type;
- game read latency;
- planner latency separately;
- percentage of zero-planner turns;
- planner logical calls per 20 turns;
- number of duplicate tasks suppressed;
- number of uncertain attempts and automatic reconciliations;
- manual interventions;
- end-turn transition duration.

## 12. Pull-request slicing

Recommended PR sequence:

1. characterization tests and inventory;
2. canonical domain types;
3. observation normalization boundary;
4. tick state and outcomes;
5. one-mutation enforcement;
6. action-attempt persistence and verification;
7. task idempotency and slot constraints;
8. decision gaps and planner eligibility;
9. plan leases and batching;
10. persistence migration;
11. frontend state exposure;
12. removal of patch layers.

Each PR SHOULD have one primary architectural claim that tests can prove.

## 13. PR checklist

Every refactor PR MUST answer:

- Which constitution invariant does this implement or preserve?
- Which state transitions change?
- Does it change planner-call eligibility or budget?
- Does it change a mutation precondition, postcondition, or retry class?
- Does it change durable schema or restart behavior?
- Can duplicate delivery or stale observation create a second effect?
- Which compatibility path is added or removed?
- What is the rollback strategy?
- Which tests prove the change?

## 14. Stop conditions

Pause the refactor slice rather than improvising when:

- current database meaning cannot be determined;
- an action’s commit semantics are unknown;
- upstream game state contradicts expected postconditions;
- two implementations disagree on authoritative behavior;
- session identity cannot be verified;
- a migration would discard unresolved attempt or approval state;
- safety depends only on planner prompt compliance.

Record the ambiguity as a focused issue or design decision before continuing.

## 15. Completion criteria

The architectural refactor is complete when:

- the canonical state machine owns all workflow execution;
- the canonical domain contracts own persisted meaning;
- one mutation per tick is structurally enforced;
- mutation verification is fresh-observation based;
- ordinary planned turns make zero planner calls;
- the planner is called only through the documented eligibility gate;
- plan leases permit multi-turn deterministic continuation;
- import-time patch layers and parallel engines are removed;
- restart, replay, concurrency, and live-game tests pass;
- documentation and frontend reflect the actual runtime, not a future design.