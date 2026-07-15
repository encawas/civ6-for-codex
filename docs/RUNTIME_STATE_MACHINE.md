# Runtime State Machine

## 1. Purpose

This document defines the required workflow control model. The runtime MUST be implemented as explicit states and transitions rather than one recursive or monolithic “run the whole turn” function.

A tick is a bounded unit of work. It may gather several read-only facts, but it performs at most one game mutation and then returns control to the caller.

## 2. Runtime states

The minimum canonical states are:

| State | Meaning | Game mutation allowed? |
|---|---|---:|
| `OBSERVING` | Read and normalize the minimum required game state | No |
| `RECONCILING` | Resolve prior action attempts, stale tasks, and disappeared events | No |
| `ROUTING` | Select the next current event, task, decision gap, or end-turn candidate | No |
| `GATHERING_CONTEXT` | Run focused read-only queries for one decision group | No |
| `REQUESTING_PLAN` | Submit one logical planner request and validate its result | No |
| `READY_TO_ACT` | Recheck preconditions for one selected task | No |
| `ACTION_SENT` | A mutation has been delivered and recorded; current tick must end | Already used |
| `VERIFYING` | Await a fresh observation proving or disproving the previous mutation | No |
| `AWAITING_APPROVAL` | A task or plan requires user approval | No |
| `AWAITING_HUMAN` | Automation cannot safely determine the next state | No |
| `PLANNER_BACKOFF` | Planner is temporarily unavailable or budget-limited | No |
| `READY_TO_END_TURN` | End-turn conditions have been checked against the current observation | No |
| `TURN_TRANSITIONING` | `end_turn` was sent; await a strictly higher turn number | No |
| `PAUSED` | User or policy paused the workflow | No |
| `SYSTEM_ERROR` | Connection, schema, lock, or invariant failure prevents safe progress | No |

A concrete implementation MAY split these states further, but MUST NOT merge mutation delivery and verification into the same state transition.

## 3. Tick contract

Every tick MUST:

1. acquire or renew exclusive ownership for the game session;
2. load durable runtime state;
3. perform the transition permitted by the current state;
4. persist the new state and audit record;
5. release execution control.

A tick MUST NOT loop indefinitely until the turn ends. Continuous mode is an external loop that invokes bounded ticks repeatedly.

## 4. Top-level transition order

The runtime MUST prioritize recovery and safety before new decisions:

```text
load durable state
→ verify game-session identity and ownership
→ observe minimum required state
→ reconcile previous mutation or turn transition
→ stop on uncertainty, approval, human wait, pause, or system error
→ invalidate stale plans/tasks
→ route one current item
→ gather focused context OR request plan OR execute one task OR end turn
→ persist state
→ end tick
```

The runtime MUST NOT call the planner while a previous mutation is unresolved.

## 5. Observation revisions

Each normalized observation MUST have a durable revision identity containing at least:

- game session identifier;
- turn number;
- monotonic observation sequence or timestamp;
- source/API version;
- hashes or revisions for relevant entity collections where practical.

Tasks, planner requests, approvals, and action attempts MUST reference the observation revision on which they were based.

A stale observation MUST NOT authorize a mutation when a newer relevant observation exists.

## 6. Minimum-state observation

The base observation SHOULD include:

- session identity and current turn;
- city production summary;
- research and civic slots;
- game blockers and modal interactions;
- durable workflow status;
- lightweight unit summary.

The lightweight unit summary SHOULD expose counts and changed identifiers without requiring full unit payloads. Full unit data is loaded only when:

- units need orders;
- a unit plan/task exists;
- the game has no city and a settler may require action;
- a relevant unit changed;
- the selected event or verification contract references a unit.

The blocker interface MUST NOT be treated as the sole authority for whether unit detail is required.

## 7. Reconciliation before routing

Reconciliation MUST occur before creating or executing new work.

It includes:

- verifying an `ACTION_SENT`, `VERIFYING`, or `UNCERTAIN` attempt;
- confirming or rejecting `TURN_TRANSITIONING`;
- resolving events no longer present in the current observation;
- cancelling tasks whose subjects disappeared;
- superseding tasks made obsolete by current game facts;
- invalidating plans whose conditions or revisions no longer hold;
- clearing stale locks only through lease/fencing rules.

Historical uncertainty MUST NOT keep an event open when current game facts conclusively satisfy the action postconditions.

## 8. Selecting one operation

After reconciliation, the router chooses exactly one of these outcomes:

1. stop in a terminal/wait state;
2. execute one ready deterministic or approved task;
3. gather one batch of focused read-only context;
4. request one strategic plan for a bounded decision group;
5. attempt `end_turn`;
6. stop with `NO_SAFE_ACTION` or equivalent diagnostic status.

The router MUST NOT select multiple mutations for one tick.

## 9. Task materialization and scheduling

Plans and rules MAY materialize tasks during routing, but the following rules apply:

- A task is created only if current facts make the slot actionable.
- A non-empty production, research, or civic slot prevents an ordinary task targeting that slot.
- Equivalent active tasks are deduplicated before insertion.
- Creating a new task SHOULD end the tick unless the task was already fully validated against the same fresh observation and execution remains structurally one operation. The preferred migration target is to execute it in the next tick.
- Scheduler conflicts are a final guard, not the primary method for handling invalid task generation.

## 10. Mutation delivery protocol

Before calling a mutating game operation, the runtime MUST:

1. select exactly one task;
2. ensure no unresolved action attempt exists for the session;
3. load the latest relevant observation;
4. evaluate all preconditions;
5. verify approval and execution mode;
6. persist an action-attempt record with status `PREPARED`;
7. assign an idempotency key and request identifier where supported.

The call then occurs outside any database transaction that would remain open across the external request.

After the call:

- a definitive local validation failure becomes `REJECTED_BEFORE_SEND` or `FAILED` as appropriate;
- a confirmed provider/game rejection becomes `FAILED`;
- a success response becomes `VERIFYING`, not `SUCCEEDED`;
- timeout, disconnect, malformed response, or ambiguous delivery becomes `UNCERTAIN` unless the action contract explicitly proves it was not sent;
- the runtime persists the result and ends the tick.

No other mutation may occur in that tick.

## 11. Action-attempt lifecycle

The canonical lifecycle is:

```text
PREPARED
  ├─> REJECTED_BEFORE_SEND
  ├─> FAILED
  ├─> VERIFYING
  └─> UNCERTAIN

VERIFYING
  ├─> SUCCEEDED
  ├─> FAILED
  ├─> VERIFYING   (more read-only evidence needed)
  └─> UNCERTAIN

UNCERTAIN
  ├─> SUCCEEDED   (later facts prove commit)
  ├─> FAILED      (later facts prove no commit and retry policy permits a new attempt)
  └─> AWAITING_HUMAN
```

`SUCCEEDED`, `FAILED`, and `REJECTED_BEFORE_SEND` are terminal for that attempt. A retry, when permitted, is a new attempt linked to the previous one.

## 12. Verification protocol

Verification MUST use a fresh observation obtained after mutation delivery. Tool return values MAY be stored as evidence but are not sufficient alone.

Each action contract MUST define positive and negative evidence.

Examples:

### Set city production

Positive evidence:

- target city still exists;
- `currently_building` normalizes to the requested item.

Negative evidence:

- city exists and reports a different newly selected item after state stabilization;
- target item is no longer legal and no requested state is present.

### Found city

Positive evidence MAY combine:

- original settler absent;
- owned city present at target coordinate;
- owned city count increased;
- target tile ownership changed consistently.

One missing signal SHOULD trigger additional read-only verification rather than a resend.

### End turn

Positive evidence:

- observed turn number is strictly greater than the recorded pre-attempt turn.

A tool-level success with the same turn number leaves the runtime in `TURN_TRANSITIONING` or escalates to uncertainty after policy limits.

## 13. Planner transition

The runtime may enter `REQUESTING_PLAN` only if:

- no executable deterministic/approved task has priority;
- no unresolved mutation exists;
- one or more strategic decision gaps remain;
- focused context requirements are complete;
- planner policy and budget permit a logical request.

Planner output is validated and persisted as plan/task proposals. It MUST NOT be executed in the same control path as the planner call unless a future explicit contract proves safety; the required default is to end the tick and re-observe before execution.

## 14. Approval transition

When a proposal requires approval:

```text
REQUESTING_PLAN or ROUTING
→ persist proposed plan/task
→ AWAITING_APPROVAL
→ end tick
```

Approval MUST create a durable revision. Editing an approved proposal creates a new revision; it MUST NOT mutate the already-audited proposal in place without history.

Rejection, cancellation, edit, and replan are distinct transitions.

## 15. End-turn transition

`READY_TO_END_TURN` is entered only after current-turn blockers and mandatory tasks are evaluated against the current observation.

The runtime then:

1. records current turn `N`;
2. persists a prepared end-turn attempt;
3. sends `end_turn` as the tick’s only mutation;
4. enters `TURN_TRANSITIONING`;
5. ends the tick.

A later tick confirms success only when the observed turn is greater than `N`.

## 16. Terminal states for a tick

The current tick MUST stop immediately after entering any of:

- `ACTION_SENT` / `VERIFYING` after a new mutation;
- `AWAITING_APPROVAL`;
- `AWAITING_HUMAN`;
- `PLANNER_BACKOFF`;
- `TURN_TRANSITIONING` after sending end turn;
- `PAUSED`;
- `SYSTEM_ERROR`.

## 17. Structural enforcement

The one-mutation invariant SHOULD be enforced by architecture, not convention. Recommended mechanisms include:

- a per-tick mutation budget object initialized to one;
- a game-port wrapper that rejects a second mutating call;
- an explicit `TickOutcome` union with one action result;
- tests using a recording game port that fails on a second mutation.

The runtime MUST NOT rely only on prompt instructions or code review to maintain this invariant.