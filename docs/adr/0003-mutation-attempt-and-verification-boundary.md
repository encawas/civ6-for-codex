# ADR 0003: Mutation Attempt and Verification Boundary

- Status: Accepted
- Date: 2026-07-15
- Scope: Tick execution, persistence, retries, verification, end turn

## Context

The current engine may create tasks, execute several due tasks, verify them, invoke the planner, and end the turn in one Tick（单次工作流步进）.

This makes ordinary turns faster in the happy path, but creates unacceptable ambiguity around:

- which observation authorized each mutation;
- whether a process crash happened before or after external delivery;
- whether an irreversible action may already have committed;
- which action caused a later state change;
- whether `end_turn` actually advanced the game;
- how to recover without duplicate effects.

The current uncertain-commit protection is valuable, but it is implemented after the fact through task state and error-message classification. It needs a structural delivery model.

## Decision

### One mutation maximum

A Tick may perform any number of reads and local persistence operations, but may deliver at most one game mutation.

`end_turn` is a mutation and consumes the same budget.

### Materialization boundary

If a Tick creates a new executable task from an observation or plan, it returns without delivering that task.

A later Tick must obtain a fresh observation and re-evaluate the task preconditions.

### Persist before send

Before calling the game port, the runtime persists an `ActionAttempt`（动作尝试）record with:

```text
attempt ID
task ID
prepared observation ID
normalized arguments
request/idempotency identifiers
retry classification
status = PREPARED
```

The record transition immediately before or during delivery must allow recovery to determine whether delivery is:

```text
proven not sent
possibly sent
acknowledged
explicitly rejected
```

### End Tick after delivery

After a mutation delivery returns or throws, the runtime persists the best-known attempt state and ends the Tick.

It does not:

- execute another mutation;
- invoke the planner;
- call `end_turn` after another action;
- mark the task successful solely from the tool return;
- perform business-as-usual planning based on a potentially unstable state.

### Later-Tick verification

A later Tick reads a fresh observation and evaluates typed, versioned postconditions.

Possible outcomes:

```text
SUCCEEDED
still VERIFYING
FAILED with proof of non-commit or impossible postcondition
UNCERTAIN
human reconciliation required
```

Repeated verification may perform reads, but must not resend a `NEVER_BLIND_RETRY` action.

### End-turn specialization

`end_turn` uses the same attempt lifecycle.

Its success postcondition is:

```text
observed turn number > recorded pre-send turn number
```

Tool acknowledgement alone is not success.

While an end-turn attempt is verifying, the runtime enters `TURN_TRANSITIONING` and performs only transition reconciliation.

## Retry classes

Every action contract declares one of:

### IDEMPOTENT_OR_DEDUPED

Retry is safe under the action's explicit idempotency mechanism.

### SAFE_IF_PROVEN_NOT_SENT

Retry is allowed only when the runtime has evidence that delivery did not occur.

### NEVER_BLIND_RETRY

Unknown delivery or commit state must be reconciled from observations or escalated to a human.

City founding, builder improvements, purchases, diplomacy acceptance, envoy use, and other irreversible/resource-consuming actions default to `NEVER_BLIND_RETRY`.

## Consequences

### Positive

- every game state change has one candidate cause per Tick;
- crash recovery is explicit;
- irreversible actions cannot be duplicated by generic retry code;
- verification becomes replayable and auditable;
- end-turn correctness uses observed game progress;
- continuous mode becomes a scheduler of bounded Ticks rather than one long transaction.

### Negative

- a turn may require more Ticks;
- tests and frontend need to expose intermediate states;
- persistence schema becomes more detailed;
- fast paths cannot combine production, unit actions, and end turn in one Tick.

The latency tradeoff is intentional: Ticks without planner calls and with local deterministic logic are expected to be fast, while correctness is protected at mutation boundaries.

## Rejected alternatives

### Allow several “independent” mutations per Tick

Rejected because independence is difficult to prove when game blockers, resources, unit positions, and turn state can change after every action.

### Verify immediately and continue when verification succeeds

Rejected because it still leaves crash windows and permits later actions to depend on state changed inside the same Tick.

### Use tool success as success for reversible actions only

Rejected as a general rule because upstream acknowledgement semantics are not uniform. Action contracts may optimize later only after real-game evidence.

## Acceptance tests

- a recording port fails if a second mutation is attempted in one Tick;
- newly materialized tasks are not delivered until a later Tick;
- the attempt record exists before game-port invocation;
- process crash after possible delivery never returns an irreversible task to `READY`;
- tool acknowledgement yields `VERIFYING`, not success;
- later observation can reconcile an uncertain action to success;
- repeated verification sends no mutation;
- `end_turn` is confirmed only by a strictly increased turn number;
- planner invocation is impossible after mutation delivery in the same Tick;
- restart during `PREPARED`, delivery-unknown, `VERIFYING`, and `TURN_TRANSITIONING` has defined behavior.
