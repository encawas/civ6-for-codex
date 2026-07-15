# Refactor Constitution

## Status

This document is normative. The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** describe implementation requirements.

The purpose of the refactor is not merely to rearrange modules. It is to produce a workflow runtime that can advance ordinary Civilization VI turns quickly without asking a model every turn, while remaining recoverable after stale reads, process restarts, transport failures, and uncertain game mutations.

## 1. System objective

The runtime MUST implement this control split:

```text
Planner: decide goals and resolve strategic ambiguity
Runtime: maintain durable workflow state and choose the next permitted operation
Rules: continue approved plans and resolve routine decisions
Game port: read or mutate the game through narrow typed operations
Frontend: supervise, approve, reject, edit, pause, and inspect
```

The planner is not the driver of each turn. The expected planner-call count for an ordinary turn is zero.

## 2. Sources of authority

### 2.1 Game facts

A fresh normalized game observation is the sole authority for current game facts, including:

- current turn;
- existing cities and units;
- active production, research, and civic choices;
- legal actions and blockers;
- positions, ownership, resources, and visible diplomacy state.

Persisted plans, tasks, events, and previous snapshots MUST NOT overwrite current observed facts.

### 2.2 Durable workflow intent

The database is authoritative only for workflow intent and history, including:

- approved plans;
- task lifecycle;
- approvals;
- action-attempt records;
- planner request records;
- unresolved uncertainty;
- workflow locks and revisions.

### 2.3 Configuration

Configuration determines permission boundaries and budgets. Runtime code MUST NOT silently widen configured action permissions, planner budgets, approval modes, or retry policies.

## 3. Domain separation

The implementation MUST represent the following concepts separately:

| Concept | Meaning |
|---|---|
| Observation | Normalized facts read from the current game |
| Event | A meaningful current condition or state change derived from observations |
| Decision gap | An event that rules and valid plans cannot currently resolve |
| Plan | Durable intent spanning one or more future actions |
| Task | A concrete candidate operation with explicit execution conditions |
| Action attempt | One recorded delivery of a mutating operation to the game port |
| Verification result | Evidence from a later fresh observation that confirms or rejects an attempt |

A class or database row MAY carry references between these concepts, but MUST NOT collapse them into one generic “workflow item.”

## 4. Tick invariants

A workflow tick MUST satisfy all of these invariants:

1. It MAY perform multiple read-only operations.
2. It MUST perform at most one game mutation.
3. `end_turn` counts as a game mutation.
4. It MUST NOT call the planner after sending a game mutation.
5. It MUST NOT send a second mutation while another mutation awaits verification or has an uncertain outcome.
6. It MUST stop when entering approval wait, human wait, system error, planner backoff, verification wait, or turn-transition state.
7. Every decision MUST be based on a named observation revision.
8. Every mutation MUST be preceded by a final precondition check against the latest available observation.

The “one mutation” constraint is an attribution and recovery boundary, not a requirement to call the planner once per tick.

## 5. Plan and task invariants

### 5.1 Plans

A plan MUST declare:

- scope, such as strategy, city, unit, builder, research, or civic;
- subject identifiers where applicable;
- revision;
- creation source and approval state;
- validity horizon;
- explicit invalidation conditions;
- intended outcomes, not only low-level tool calls.

A plan MUST NOT execute itself.

### 5.2 Task materialization

A task MAY be materialized from a plan only when:

- the plan is active and valid;
- the relevant game slot is available;
- task preconditions currently hold;
- no equivalent active task already exists;
- the task is within its execution window;
- approval requirements are satisfied or the task is created as awaiting approval.

Current game selections are facts. They MUST prevent creation of ordinary replacement tasks unless an explicit approved replacement operation exists.

### 5.3 Idempotency

Equivalent repeated observations MUST NOT create duplicate active tasks. Every task MUST have a stable idempotency identity derived from the game session, subject, slot, intended outcome, and relevant plan revision.

## 6. Mutation safety

Every mutating action type MUST define:

- required arguments;
- allowed entity types;
- preconditions;
- postconditions;
- approval policy;
- whether it is safe to retry after an unknown outcome;
- reconciliation queries;
- timeout and backoff policy.

Before delivery, the runtime MUST persist an action-attempt record sufficient to recover after a crash.

After delivery, the runtime MUST NOT treat a tool-level `success` response as final proof. Completion requires later postcondition evidence from a fresh observation.

For non-idempotent or irreversible actions, a timeout, disconnect, malformed response, or unverified success MUST enter a protected verification or uncertain state. Blind resend is forbidden.

## 7. Planner boundaries

The planner MUST:

- receive only focused context required for named decision gaps;
- return structured proposals, plan updates, or information requests;
- use only allowed action and condition types;
- attach decisions to observation and plan revisions;
- explain which decision gap each output resolves.

The planner MUST NOT:

- call MCP or other game tools;
- mutate the database directly;
- own workflow retries;
- decide whether an action was actually committed;
- receive the full raw game state by default;
- be called merely because a new turn began;
- produce routine movement for every unit when a rule or approved plan can continue safely.

## 8. Approval boundaries

Approval applies to strategic commitment, not necessarily every mechanical step.

For example, approving “move this settler to `(25,31)` and found a city if conditions remain valid” MAY authorize safe path continuation across several turns. The runtime MUST pause and re-request a decision if the path, threat level, target validity, or approved conditions materially change.

Approval, rejection, edit, cancellation, and replan MUST be first-class durable transitions.

## 9. End-turn boundary

The runtime MAY attempt `end_turn` only when:

- no current game hard blocker remains;
- no mandatory current-turn task is ready;
- no action is executing, verifying, or uncertain;
- no required approval or human decision is pending;
- no planner result required for this turn is pending;
- configured execution mode permits automatic end turn.

An `end_turn` attempt is confirmed only when a later fresh observation reports a turn number strictly greater than the recorded pre-attempt turn.

## 10. Persistence and restart

All state required to decide whether another mutation is safe MUST survive process restart.

The runtime MUST recover deterministically from restart during:

- task preparation;
- action delivery;
- verification wait;
- uncertain outcome;
- approval wait;
- planner backoff;
- turn transition.

Database migrations MUST be versioned. New code MUST NOT infer incompatible schema meaning from the existence of old columns alone.

## 11. Concurrency

Only one runtime owner MAY mutate a given game session at a time. Ownership MUST use a durable lease or lock with expiry and fencing/version semantics.

A second process MUST fail closed rather than assuming the first process is dead.

## 12. Observability

Logs and metrics MUST separate:

- game observation latency;
- normalization latency;
- event derivation;
- rule routing;
- focused information queries;
- logical planner requests and provider retry attempts;
- task materialization;
- action delivery;
- verification;
- approval wait;
- turn transition.

A generic “agent timeout” is insufficient.

## 13. Refactor constraints

The refactor MUST converge toward one canonical implementation for each responsibility.

The following are prohibited as end-state architecture:

- adding more `safe_*` shadow implementations;
- import-time monkey-patching to replace public classes;
- multiple engines selected by hidden import order;
- duplicated schemas whose meanings drift;
- a single giant tick method containing observation, planning, execution, verification, and end-turn logic;
- planner prompts encoding safety rules that are not also enforced in code;
- direct MCP calls outside the game-port/action boundary.

Temporary compatibility adapters MAY exist only when they have a documented removal phase and tests proving both sides of the migration boundary.

## 14. Definition of done

The refactor is complete only when all of the following are demonstrated:

- ordinary planned turns complete with zero planner calls;
- strategic changes produce at most the allowed logical planner requests;
- repeated observations do not duplicate tasks;
- one mutation per tick is enforced structurally;
- irreversible actions cannot be blindly retried;
- uncertain attempts can later reconcile automatically from game facts;
- restart tests cover every protected state;
- no runtime behavior depends on import-time implementation replacement;
- the frontend exposes the current workflow state, blocking reason, planner usage, pending approval, and last verified mutation.