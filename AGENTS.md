# Coding-agent instructions

This repository is entering an architectural refactor. The documents listed below are normative constraints, not optional design suggestions.

## Authority order

When documents or existing code disagree, use this order:

1. `docs/REFACTOR_CONSTITUTION.md`
2. `docs/RUNTIME_STATE_MACHINE.md`
3. `docs/PLANNER_CALL_POLICY.md`
4. `docs/DOMAIN_CONTRACTS.md`
5. `docs/REFACTOR_EXECUTION_PLAN.md`
6. `docs/WORKFLOW_AGENT_ARCHITECTURE.md`
7. Existing implementation details

Do not silently preserve behavior that violates a higher-authority document. Record the incompatibility, add a characterization test where useful, and migrate it explicitly.

## Non-negotiable rules

1. The current game observation is the source of truth. Database records may explain or constrain actions, but must not overwrite observed game facts.
2. Keep these concepts distinct: observation, event, decision gap, plan, task, action attempt, and verification result.
3. One workflow tick may perform many reads but at most one game mutation. `end_turn` is a game mutation.
4. After sending a mutation, persist the attempt and end the tick. Verification occurs from a fresh observation in a later tick.
5. An irreversible or non-idempotent action must never be blindly retried after an unknown outcome.
6. A plan is not an executable command. It may create a task only when current preconditions are satisfied and no equivalent active task exists.
7. Existing game choices are facts, not scheduler candidates. For example, a non-empty research slot prevents creation of a normal `set_research` task.
8. The default planner-call count for an ordinary turn is zero. A logical planner request is allowed only for an unresolved strategic decision gap and is capped by `docs/PLANNER_CALL_POLICY.md`.
9. The planner cannot call MCP tools, mutate the game, own retries, or maintain authoritative runtime state.
10. Waiting for approval, unresolved mutation outcome, system failure, and turn transition are terminal states for the current tick.
11. Do not add another `safe_*`, overlay, monkey-patch, import-time replacement, or parallel runtime implementation. The refactor must converge toward one canonical implementation per responsibility.
12. Do not perform a big-bang rewrite. Preserve executable behavior through characterization tests and migrate one boundary at a time.

## Required change discipline

Before changing runtime behavior:

- identify the affected invariant and state transition;
- add or update tests for the old observable behavior and the intended contract;
- state whether the change affects persistence, idempotency, planner-call frequency, approval, or end-turn safety;
- ensure restart/reload behavior is covered when durable state changes;
- ensure logs and metrics distinguish observation, planning, execution, and verification latency.

A change is incomplete if it only makes the happy path pass while leaving crash recovery, duplicate delivery, stale state, or unknown action outcome undefined.

## Minimum test gates

Every refactor slice must keep or add tests covering the relevant subset of:

- zero planner calls on an ordinary planned turn;
- no duplicate task creation from repeated identical observations;
- no task creation for an already occupied slot;
- one mutation maximum per tick;
- fresh-observation verification after mutation;
- no blind retry of an irreversible action;
- automatic reconciliation when a later observation proves an uncertain action succeeded;
- approval and human-wait states blocking end turn;
- `end_turn` confirmed only by a strictly increased turn number;
- process restart during pending, verifying, uncertain, and turn-transition states;
- stale plan invalidation through version and precondition checks.

## Completion standard

Prefer a smaller, internally consistent vertical slice over a broad partial rewrite. New code must make the valid path obvious and invalid state combinations difficult or impossible to represent.