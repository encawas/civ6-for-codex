# Characterization Test Catalog

## 1. Purpose

This catalog defines the tests that must freeze verified behavior before the runtime architecture is replaced.

A characterization test answers:

> What does the current system observably do, and which part of that behavior must the refactor preserve or intentionally replace?

Not every current behavior is desirable. Tests in this catalog therefore carry a disposition:

- `PRESERVE`: required safety or verified game behavior;
- `MIGRATE`: behavior must remain available while its implementation changes;
- `REPLACE`: current behavior is recorded so the new invariant can intentionally differ;
- `INVESTIGATE`: current semantics are not yet reliable enough to declare.

## 2. Test harness conventions

### 2.1 Test identifiers

Use stable identifiers in test docstrings or markers:

```text
IMP-*  import/composition
OBS-*  observation and normalization
EVT-*  event projection and reconciliation
PLAN-* plan lifecycle and leases
TASK-* task materialization and idempotency
ACT-*  action delivery and attempts
VER-*  verification and reconciliation
TURN-* end-turn safety and transition
AI-*   planner eligibility and budgeting
APR-*  approval and human interaction
REC-*  restart and recovery
SES-*  game-session identity and concurrency
MET-*  metrics and observability
WEB-*  control API/frontend contract
```

### 2.2 Two assertion layers

Each critical scenario should separate:

1. **observable outcome assertions** — state, task, event, planner call, mutation call;
2. **implementation-independent invariant assertions** — no duplicate effect, no unsafe retry, no unverified success.

### 2.3 Recording test doubles

Provide reusable fakes:

```text
RecordingGamePort
ScriptedSnapshotSource
RecordingPlanner
FailingPlanner
DeterministicClock
CrashInjector
```

`RecordingGamePort` must classify calls as:

```text
READ
MUTATION
END_TURN_MUTATION
```

It should optionally fail immediately when a second mutation occurs in one Tick.

### 2.4 Replay fixtures

Store representative normalized or adapter-input fixtures under a versioned path such as:

```text
tests/fixtures/replays/v1/
```

Each fixture should record:

- source/protocol version;
- game/session identity inputs;
- turn;
- expected normalization version;
- whether units are included;
- expected events;
- expected deterministic tasks;
- expected planner eligibility;
- expected mutation allowance.

## 3. Existing coverage to retain

The repository already contains valuable tests around:

- settler site selection;
- approved settler movement;
- city founding postconditions;
- focused read-only information queries;
- explicit event resolutions;
- event disappearance reconciliation;
- uncertain irreversible actions;
- planner provider failure/backoff;
- fail-closed blocking behavior.

During migration, do not delete these tests merely because their imports or class names change. Move them to canonical adapters or rewrite their setup while retaining their observable claims.

## 4. Import and composition tests

### IMP-001 — effective public engine class

**Disposition:** MIGRATE

Assert the class currently exposed as `civ6_workflow.engine.WorkflowEngine` resolves to the commit-safe workflow implementation after package import.

Purpose: record the hidden composition behavior before removing it.

### IMP-002 — direct base engine remains different

**Disposition:** REPLACE

Record that the source-defined base engine and the package-exposed engine are not semantically identical.

The replacement target is explicit bootstrap wiring, after which this test should be deleted and replaced by bootstrap composition tests.

### IMP-003 — registry mutation inventory

**Disposition:** REPLACE

Assert current package import installs:

- `unit_found_city` action;
- extended condition types;
- workflow condition evaluator;
- safe store;
- settler compiler;
- safe MCP port;
- enhanced web classes.

This test is evidence for removal, not a permanent architectural requirement.

### IMP-004 — bootstrap has no hidden import mutation

**Disposition:** PRESERVE as target invariant

Initially mark expected failure. It passes only when importing domain modules does not mutate unrelated modules or registries.

## 5. Observation and normalization tests

### OBS-001 — all empty production spellings normalize identically

Inputs:

```text
None
""
"NONE"
"none"
"nothing"
"NOTHING"
{}
[]
```

Expected canonical result:

```text
production slot is empty
```

No core rule should contain these spellings after normalization cutover.

### OBS-002 — occupied production remains occupied

Cover representative unit, building, district, project, and wonder identifiers.

### OBS-003 — normalization is adapter-owned

A static or architecture test should reject imports from concrete upstream payload helpers inside domain rule modules.

### OBS-004 — unit summary triggers detail read

Given a light observation reporting units needing orders, the runtime requests unit detail before routing or executing unit work.

### OBS-005 — zero-city state requires settler/unit inspection

Even if blocker data is empty, zero cities must cause sufficient unit observation to discover the opening settler.

### OBS-006 — immutable observation identity

Two reads of equal game facts receive distinct observation IDs but equal relevant projection hashes.

## 6. Event tests

### EVT-001 — repeated condition has one open event

Repeated identical observations update `last_seen` and `seen_count`; they do not create duplicate open events.

### EVT-002 — stable strategic dedupe across turns

An unresolved settler site-selection question for the same unit keeps one semantic event identity across turns.

This test should fail against the current turn-number-based dedupe behavior and is a required replacement invariant.

### EVT-003 — turn-specific event may include turn identity

A genuinely turn-specific condition, such as a one-turn tactical opportunity, may intentionally use turn in its identity. The event type must declare that policy explicitly.

### EVT-004 — disappeared ordinary condition resolves automatically

A city-production-empty event resolves after a later observation shows production selected.

### EVT-005 — event status does not override game fact

A historically open event cannot force the runtime to treat a currently resolved game condition as still true.

### EVT-006 — uncertain action event remains separate from source blocker

The original blocker may resolve while an action attempt remains uncertain. Assert that event and attempt lifecycles do not overwrite one another.

## 7. Plan tests

### PLAN-001 — plan is not executable by itself

Saving an active research or city plan does not mutate the game.

### PLAN-002 — occupied slot suppresses task materialization

Given current research `TECH_MINING` and queue head `TECH_POTTERY`, no normal `set_research` task is created.

### PLAN-003 — plan lease supports zero-planner continuation

Across several stable turns, an active plan remains valid and deterministic continuation occurs with zero logical planner requests.

### PLAN-004 — scope-specific invalidation

Invalidating one settler plan does not invalidate unrelated city production or research plans.

### PLAN-005 — plan revision supersedes old task source

A task tied to an old plan revision cannot execute after a new revision supersedes it unless explicitly carried forward.

### PLAN-006 — review turn is not automatic planner eligibility

A review deadline creates a review candidate. The runtime may extend the lease without planner invocation when relevant facts and policy have not materially changed.

## 8. Task tests

### TASK-001 — repeated observation does not duplicate task

Equivalent desired work under the same active plan revision creates one active task.

### TASK-002 — different task IDs cannot duplicate semantic work

Two proposals with different IDs but the same semantic idempotency key must conflict or deduplicate.

This extends current same-ID conflict protection.

### TASK-003 — slot conflict detected before scheduling

Two active tasks targeting the same entity/slot/execution window cannot both become `READY`.

### TASK-004 — new task ends materialization Tick

When a Tick creates a new task, it performs no game mutation and returns a task-materialized outcome.

### TASK-005 — stale observation prevents execution

A task created from an older observation must revalidate against a fresh observation before delivery.

### TASK-006 — current-turn requirement blocks end turn

A required task with `must_complete_before_end_turn = true` blocks end turn even if it is not currently `READY` because it awaits approval or verification.

## 9. Mutation and action-attempt tests

### ACT-001 — at most one mutation per Tick

Configure multiple ready tasks. Assert exactly one mutation is delivered and remaining tasks stay ready.

### ACT-002 — end turn counts as a mutation

If another mutation was delivered in the Tick, `end_turn` cannot also be called.

### ACT-003 — attempt persisted before delivery

The fake game port inspects the store when called and asserts an attempt record already exists in `PREPARED` or equivalent sent-capable state.

### ACT-004 — crash before send is retryable

Inject a crash after attempt preparation but before the game port is called. Recovery may retry because delivery is proven not to have occurred.

### ACT-005 — crash after send is not blindly retryable

Inject a crash immediately after the game port receives an irreversible action. Recovery must reconcile before any resend.

### ACT-006 — transport failure classification

Distinguish:

- connect failure before request delivery;
- connection loss during/after delivery;
- explicit tool rejection;
- tool acknowledgement without observable result.

No safety decision may depend only on matching an error-message prefix.

### ACT-007 — action registry is canonical and immutable after bootstrap

Runtime execution uses one registry instance. Tests fail if action types are installed through unrelated import side effects.

## 10. Verification tests

### VER-001 — tool success becomes verifying, not succeeded

After a mutation returns success, the task/attempt state is `VERIFYING`; success requires a later observation.

### VER-002 — verification occurs in a later Tick

The Tick that delivers a mutation performs no post-delivery game read used to mark success.

### VER-003 — later observation proves success

An uncertain city founding or builder improvement becomes succeeded when later observations satisfy typed postconditions.

### VER-004 — failed verification does not resend irreversible action

Multiple verification Ticks may read repeatedly but mutation count remains one.

### VER-005 — safe retry requires explicit classification

An idempotent or upstream-deduplicated action may retry only through its action contract and attempt lineage.

### VER-006 — postcondition version is recorded

Changing condition semantics cannot silently reinterpret historical attempts.

## 11. End-turn tests

### TURN-001 — tool acknowledgement is not confirmation

If `end_turn` returns success but the observed turn remains `N`, `turn_ended` stays false.

### TURN-002 — strictly increased turn confirms transition

A later observation with turn `> N` confirms the transition.

### TURN-003 — transition state blocks all other work

While awaiting turn change, the runtime only performs transition reconciliation reads; it creates no tasks, calls no planner, and sends no mutation.

### TURN-004 — approval blocks end turn

Any current-turn-required approval state blocks end turn.

### TURN-005 — uncertain mutation blocks end turn

An unresolved possibly committed action blocks end turn.

### TURN-006 — future task does not necessarily block end turn

A task whose execution window begins in a later turn does not block the current turn.

### TURN-007 — no planner call solely to authorize end turn

If all conditions are deterministic and clear, end-turn eligibility never invokes the planner.

## 12. Planner tests

### AI-001 — ordinary planned turn has zero logical requests

This is the primary performance invariant.

### AI-002 — routine unit skip has zero planner calls

Ordinary units with no strategic opportunity are handled by deterministic policy.

### AI-003 — one stable decision gap produces one logical request

Repeated Ticks and provider retries do not create additional logical request records.

### AI-004 — information round trip remains one logical request

An initial planner response requesting allowed information plus a final planner answer counts as:

```text
logical requests = 1
provider attempts = 2 or more
```

### AI-005 — multiple strategic gaps batch once

Compatible city, research, civic, and settler questions may be submitted in one bounded decision batch.

### AI-006 — system failures never route to planner

Connection errors, schema errors, uncertain commits, and approval waits produce runtime/human outcomes, not planner requests.

### AI-007 — unchanged input hash suppresses duplicate request

A repeated decision gap with the same relevant observation projection, plan revisions, and policy does not call the planner again before cooldown or invalidation.

### AI-008 — planner failure does not block unrelated deterministic work

After transient planner failure, safe existing plans may continue in later Ticks.

### AI-009 — provider backoff is measured separately

Assert provider backoff metadata does not masquerade as a game blocker or strategic decision gap.

## 13. Approval tests

### APR-001 — approval state is a terminal Tick outcome

Once a required approval is detected or created, the Tick returns immediately.

### APR-002 — approve changes one explicit revision

Approval is tied to exact task/plan semantics. Editing after approval creates a new revision requiring re-evaluation.

### APR-003 — reject has durable meaning

Rejected work does not reappear from the same unchanged plan/event on the next Tick.

### APR-004 — replan is distinct from reject

A replan request opens a bounded decision gap; it does not directly mutate the old task.

### APR-005 — strategic approval covers continuation policy

Approving a settler destination may authorize deterministic path continuation while city founding remains separately approval-controlled according to policy.

## 14. Restart and recovery tests

### REC-001 — current RUNNING-to-READY behavior is characterized

Record current migration behavior as legacy evidence.

Disposition: REPLACE.

### REC-002 — prepared-before-send recovery

A prepared attempt known not to be sent may return to executable state.

### REC-003 — possibly sent recovery

A possibly sent attempt enters verification/uncertain recovery and cannot become ready automatically.

### REC-004 — verifying recovery

Restart during verification resumes by reading current state, not by resending.

### REC-005 — turn-transition recovery

Restart after `end_turn` delivery resumes turn-number verification.

### REC-006 — approval recovery

Restart preserves pending approval and prevents end turn.

### REC-007 — planner transaction recovery

A recorded logical planner request with an unknown provider result does not create duplicate accepted plan revisions.

## 15. Session and concurrency tests

### SES-001 — two processes cannot mutate one FireTuner session

Preserve the current user-global serialization behavior until a stronger session lease exists.

### SES-002 — same database, duplicate HTTP Tick

Concurrent duplicate Tick requests result in one active Tick and at most one mutation.

### SES-003 — different databases, same game

Two processes using different SQLite paths still cannot mutate the same local game concurrently.

### SES-004 — different save identity pauses mutation

Loading a different game/save invalidates the active session lease and requires explicit reconciliation.

### SES-005 — turn rewind is not ordinary progression

A lower observed turn creates a recovery state and prevents stale task execution.

## 16. Metrics tests

### MET-001 — one metrics record per Tick

Multiple Ticks in the same turn remain individually queryable.

### MET-002 — latency categories are separate

Track observation, deterministic planning, mutation delivery, verification, information queries, planner provider time, and persistence separately.

### MET-003 — planner measures logical and physical calls

The UI and reports expose both counts.

### MET-004 — zero-planner-turn ratio

A replay report calculates the percentage of completed turns with zero logical planner requests.

### MET-005 — mutation invariant metric

Record mutation count per Tick and fail tests if any value exceeds one.

## 17. Recommended Phase 0 implementation order

1. Add reusable recording fakes and a mutation classifier.
2. Add IMP-001 through IMP-003 to document current composition.
3. Add OBS-001 for `nothing` normalization.
4. Add PLAN-002 for occupied research-slot suppression.
5. Add TURN-001 for unconfirmed `end_turn`.
6. Add ACT-001 as an expected-failure target invariant.
7. Add TASK-001 and TASK-002 for semantic duplication.
8. Add AI-001 and EVT-002 to expose repeat-planning risk.
9. Add REC-001 and ACT-005 for restart semantics.
10. Capture one representative opening replay.

## 18. Pull-request gate

No architectural refactor PR should be approved unless it states which catalog IDs it:

- adds;
- makes pass;
- intentionally changes;
- retires after replacing temporary behavior.
