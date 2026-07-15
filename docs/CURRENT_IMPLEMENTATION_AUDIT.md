# Current Implementation Audit

## 1. Purpose

This document records what the repository **actually executes today** before the architectural refactor begins.

It is a Phase 0 artifact. It does not describe the target architecture except where a comparison is required. When this document conflicts with the normative refactor contracts, the normative contracts win; the conflict recorded here becomes migration work.

The main conclusion is:

> The current package exposes one public runtime name, but the effective behavior is assembled from several inheritance layers and import-time replacements. The first refactor objective is therefore not to add features. It is to make the existing behavior explicit, testable, and replaceable one boundary at a time.

## 2. Effective runtime composition

Importing `civ6_workflow` currently performs runtime mutation of other modules.

The effective engine chain is:

```text
engine.WorkflowEngine
    ↓ subclassed by
safe_engine.SafeWorkflowEngine
    ↓ subclassed by
workflow_engine.WorkflowAwareEngine
    ↓ subclassed by
runtime_safety.CommitSafeWorkflowEngine
    ↓ assigned back into
engine.WorkflowEngine
```

The effective deterministic compiler chain is:

```text
rules.DeterministicRuleCompiler
    ↓ subclassed by
safe_rules.SafeDeterministicRuleCompiler
    ↓ subclassed by
settler_rules.SettlerDeterministicRuleCompiler
    ↓ assigned back into
rules.DeterministicRuleCompiler
```

The package initializer also replaces or extends:

- `models.PlanBundle`, `models.AgentRequest`, and `models.TickMetrics`;
- `actions.ACTION_REGISTRY`;
- validation condition registries;
- `conditions.ConditionEvaluator`;
- `store.WorkflowStore`;
- `mcp_port.Civ6GamePort`;
- planner system instructions;
- replay classes;
- web control-panel classes and HTML.

### Consequences

1. The class named in a source file is not necessarily the class used at runtime.
2. Import order is part of program behavior.
3. Tests importing through the package may exercise patched classes while direct module loading or tooling may inspect base classes.
4. Static analysis cannot reliably infer the active implementation from declarations alone.
5. A new `safe_*` layer can accidentally shadow an earlier fix instead of integrating it.

### Preserve during migration

The refactor must preserve the behavior currently provided by these layers until equivalent canonical implementations and characterization tests exist:

- user-global Tick（单次工作流步进）serialization;
- confirm/read-only fail-closed task persistence;
- compact planner context projection;
- focused information queries;
- provider failure classification and backoff;
- event-resolution validation;
- settler selection, movement, and founding behavior;
- uncertain-commit protection for irreversible actions;
- event disappearance reconciliation;
- strict replay behavior;
- control-panel safety behavior.

## 3. Effective Tick behavior

The base `WorkflowEngine.tick()` currently performs a whole-turn pipeline:

```text
read base snapshot
→ optionally read units
→ compile deterministic tasks
→ save compiled tasks
→ load all due tasks
→ execute every due task in a loop
→ verify every task in the same Tick
→ derive final events
→ optionally invoke planner
→ optionally call end_turn
→ return
```

### 3.1 Multiple mutations per Tick

The current task execution loop iterates over all due tasks. A Tick may therefore send multiple game mutations before returning.

Examples include:

- selecting production for more than one city;
- selecting research and civic choices;
- moving or skipping multiple units;
- executing a builder operation;
- then calling `end_turn` in the same Tick.

This violates the target invariant of at most one game mutation per Tick.

### 3.2 Same-Tick task creation and execution

Deterministic rule bundles are saved before due tasks are reloaded. Newly materialized tasks can therefore execute in the same Tick that observed and created them.

This removes the intended fresh-observation boundary between:

```text
observation used to create task
and
observation used to authorize mutation
```

### 3.3 Same-Tick verification

After a tool reports success, `_execute_one_task()` immediately performs one or more reads and evaluates postconditions inside the same Tick.

This provides useful verification behavior, but it does not establish a durable mutation boundary:

- no separate persisted action-attempt lifecycle exists;
- process termination after external delivery but before task status update is ambiguous;
- verification is not guaranteed to begin from a later Tick;
- multiple later mutations may execute after one action verifies.

### 3.4 End-turn confirmation

The base engine marks `turn_ended = true` when the `end_turn` tool reports success.

It does not require a later observation whose turn number is strictly greater than the pre-action turn.

Therefore tool acknowledgement and confirmed turn transition are currently conflated.

## 4. Current planner path

### 4.1 Eligibility

Planner invocation is based on routed blocking or high-level events and a per-turn `agent_called_for_turn` check.

The current mechanism does not yet have a first-class `DecisionGap`（决策缺口）record. Consequently:

- system blockers, strategic choices, task failures, and human-review situations can share event routing machinery;
- planner eligibility is inferred from event level and blocking status;
- a valid plan lease is not a first-class reason to suppress planning;
- unchanged strategic questions are not deduplicated by an input hash.

### 4.2 Logical request versus provider calls

`WorkflowAwareEngine` may:

1. prefetch deterministic read-only information;
2. call the planner;
3. execute planner-requested information queries;
4. call the planner a second time for a final answer.

This can be treated as one logical planning transaction, but current metrics primarily count attempted provider calls. The refactor must retain both measurements:

```text
logical_planner_request_count
provider_attempt_count
provider_success_count
information_query_count
```

### 4.3 Per-turn call budget

The base engine mostly treats `max_agent_calls_per_turn` as an enable/disable upper bound combined with a boolean “already called” record.

The actual policy is therefore closer to:

```text
zero or one logical planning transaction per turn
```

rather than a general integer budget.

This is acceptable as a transitional safety behavior, but must become explicit.

### 4.4 Planner-call amplification risk

Several unit and settler event dedupe keys include the current turn number, for example conceptually:

```text
event_type:unit_id:turn
```

An unresolved condition that persists into the next turn therefore becomes a new dedupe identity. This can bypass cross-turn cooldown and repeatedly create planner-eligible events.

Stable strategic questions should instead use stable identities such as:

```text
settler_site_selection_required:unit_id
unit_plan_requires_review:unit_id:plan_revision
```

The observation turn belongs in event metadata, not in the durable semantic identity unless the event is inherently turn-specific.

## 5. Current domain model

The present models are useful transport models but combine several target concepts.

### 5.1 RuntimeSnapshot

`RuntimeSnapshot` contains raw or semi-structured dictionaries and lists.

Gaps:

- no immutable observation identifier;
- no observation sequence or revision;
- no normalization version;
- no per-entity revision/hash;
- no explicit source versions;
- no stable session identity beyond `game_id`.

### 5.2 GameEvent

`GameEvent` combines current conditions, notification routing, severity, blocking, and dedupe metadata.

Gaps:

- no explicit lifecycle status in the model;
- no opened/resolved observation references;
- no route type separate from severity;
- no distinction between strategic decision events and runtime-recovery events.

### 5.3 PlanBundle

`PlanBundle` mixes:

- strategy updates;
- entity-plan replacement;
- task proposals;
- task cancellation;
- review timing;
- human-review flags.

Gaps:

- no plan revision;
- no plan status;
- no scope-specific lease;
- no explicit validity/invalidation contract;
- no supersession relation;
- no approval record separate from a boolean flag.

### 5.4 StoredTask

`StoredTask` is both the durable task definition and its execution state.

Gaps:

- no semantic idempotency key separate from `task_id`;
- no slot identity;
- no source observation or plan revision;
- no separate action-attempt record;
- no explicit `VERIFYING` state;
- no execution window beyond `due_turn`/`expires_turn`;
- no `must_complete_before_end_turn` field.

## 6. Current persistence behavior

### 6.1 Schema shape

The SQLite store currently persists:

- workflow metadata;
- strategy state;
- city, unit, and builder plans;
- workflow tasks;
- event log;
- agent runs;
- per-turn metrics;
- unit observations.

This is a valuable behavioral baseline and must be migrated, not discarded.

### 6.2 Restart recovery risk

The base migration currently converts tasks left in `RUNNING` back to `READY`.

That is unsafe once `RUNNING` can mean that an external mutation may have been delivered. A restarted process could resend a non-idempotent action.

Before canonical mutation delivery is introduced, restart behavior must distinguish at least:

```text
prepared but not sent
send proven to have failed before delivery
possibly sent / outcome unknown
sent and awaiting verification
```

The future action-attempt table must be persisted before delivery.

### 6.3 Task identity

`SafeWorkflowStore` rejects reusing the same `task_id` for different semantics. This protects one class of accidental overwrite.

It does not prevent equivalent work from being inserted under two different task IDs.

The canonical store therefore needs a semantic idempotency key and active-status uniqueness rule.

### 6.4 Plan replacement

Current city/unit/builder plans are upserted by entity key. Previous revisions are overwritten in the active tables.

The new model should preserve history and use revisioned plan records, while an active-plan projection may still provide efficient lookup.

### 6.5 Global metadata

Several workflow values are stored in unscoped `workflow_meta` keys, including “last” planner and information-query state.

Risks:

- switching saves can expose stale metadata from another game;
- multiple runtime sessions can overwrite one another;
- replay and live-game metadata can collide;
- provider-global backoff and game-scoped state are not clearly separated.

Metadata must be classified as:

```text
process-global
provider-global
game-session-scoped
turn-scoped
Tick-scoped
```

### 6.6 Metrics overwrite

`turn_metrics` uses `(game_id, turn)` as its primary key.

Multiple Ticks in the same turn cannot be represented as independent metric records. This hides the exact cost of:

- observation Ticks;
- task-delivery Ticks;
- verification Ticks;
- planning Ticks;
- turn-transition Ticks.

Canonical metrics require a `tick_id` primary key and an indexed `(game_session_id, turn_number)` projection.

## 7. Current normalization behavior

Normalization is not yet a single adapter-owned boundary.

Rules still contain direct comparisons such as checking whether production equals one of:

```text
None
""
"NONE"
"none"
{}
[]
```

This misses additional upstream spellings such as `nothing` and spreads protocol knowledge through business rules.

The canonical adapter should return a typed value:

```text
production_slot = Empty
```

Core rules must not know the list of upstream empty spellings.

## 8. Event reconciliation behavior

The safe store automatically resolves non-sticky events that disappear from the current Tick.

The sticky set currently includes task failures, uncertain commits, and turn rewind events.

This is intentionally conservative, but uncertain action evidence is not yet reconciled into task success automatically. A later observation may prove that the action succeeded while the task remains `UNCERTAIN` and its event remains open.

Canonical reconciliation must evaluate postconditions for unresolved attempts before routing anything to the planner or human.

## 9. Approval and end-turn interaction

The current store can persist tasks as `AWAITING_CONFIRMATION`, and explicit approval moves them to `READY`.

End-turn safety must explicitly query all turn-blocking workflow states, not only due `READY` tasks.

Required blockers include:

- approval required before the current turn can safely finish;
- task executing or verifying;
- uncertain mutation outcome;
- required current-turn task not yet materialized;
- turn transition already in progress;
- human-only decision required.

A generic “there are no due tasks” check is insufficient.

## 10. Behavior worth preserving

The following current behaviors are valuable and should be moved into canonical locations rather than removed:

| Existing behavior | Canonical destination |
|---|---|
| user-global non-blocking Tick lock | application runner / session lease |
| compact planner projection | application planner context projector |
| event-specific information queries | application query router |
| provider retry diagnostics and backoff | planner adapter |
| strict plan validation | application planner boundary |
| deterministic city/unit/builder continuation | domain policies + task materializer |
| settler plan workflow | scoped unit-plan policy |
| action registry with retry classification | canonical action contracts |
| uncertain irreversible action protection | attempt reconciler |
| event disappearance reconciliation | event projector/reconciler |
| replay ports | canonical test and replay adapters |
| localhost control panel | web adapter over canonical application state |

## 11. Behavior that must not survive unchanged

| Current behavior | Required replacement |
|---|---|
| import-time module/class replacement | explicit `bootstrap.py` composition root |
| several engine subclasses owning overlapping policy | one state machine with injected policies |
| all due tasks executed in a loop | one selected mutation maximum per Tick |
| task creation followed by immediate execution | return after materialization; re-observe next Tick |
| same-Tick mutation verification | durable later-Tick verification |
| `end_turn` success based on tool acknowledgement | verify strictly increased turn number |
| `RUNNING → READY` restart migration | action-attempt-aware recovery |
| turn number embedded in persistent strategic event identity | stable semantic dedupe key |
| task ID as sole idempotency mechanism | semantic idempotency key + active uniqueness |
| plans overwritten by entity key | revisioned plans and active projection |
| event level used as planner eligibility | first-class decision gaps and eligibility gate |
| one row of metrics per turn | one row per Tick plus turn aggregates |

## 12. Recommended canonical composition

The first canonical bootstrap should construct dependencies explicitly:

```text
config
→ clock
→ canonical store
→ Civ6 MCP adapter
→ planner adapter
→ action registry
→ condition registry
→ normalizer
→ event projector
→ reconciler
→ task materializer
→ task selector
→ planner eligibility policy
→ end-turn policy
→ bounded Tick runner
→ control API
```

No import should replace another module’s exported class or mutate a registry as a hidden side effect.

## 13. High-risk migration traps

### 13.1 Reusing the old full-turn loop inside a new node

Do not implement a new finite-state machine node whose body simply calls the existing `WorkflowEngine.tick()`. That preserves all old coupling under a new label.

### 13.2 Moving files before freezing behavior

Relocating the current classes without characterization tests will make it difficult to distinguish intended design change from accidental regression.

### 13.3 Treating every old test as a target requirement

Some tests encode temporary implementation details, including patched public imports and same-Tick verification. Classify tests as:

```text
safety invariant
verified game behavior
compatibility behavior
temporary implementation detail
obsolete behavior
```

Only the first three categories should normally constrain the migration.

### 13.4 Dual authoritative writes

A temporary compatibility projection may be required, but old and new tables must not both accept independent authoritative updates.

### 13.5 Planner-first refactor

Do not start by redesigning prompts. The major current latency and correctness issues are control-plane issues: event identity, plan validity, task materialization, mutation boundaries, and persistence.

## 14. Phase 0 exit additions

In addition to `REFACTOR_EXECUTION_PLAN.md`, Phase 0 should not close until the repository proves:

1. the effective runtime classes are recorded by a test;
2. every import-time replacement is inventoried;
3. a recording port can count and classify mutations;
4. current multi-mutation behavior is captured as legacy evidence;
5. the new invariant test fails until one-mutation enforcement exists;
6. current restart conversion of `RUNNING` is characterized;
7. stable versus turn-specific event identities are catalogued;
8. planner logical requests and provider attempts are measured separately;
9. approval, uncertain outcome, and turn transition are explicitly included in end-turn tests;
10. a real or representative 5–10 turn replay fixture is retained.
