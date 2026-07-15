# ADR 0002: Stable Decision Identity and Plan Leases

- Status: Accepted
- Date: 2026-07-15
- Scope: events, decision gaps, planner eligibility, planner-call frequency

## Context

The product goal is not to ask an AI planner every turn. The planner should make bounded strategic decisions, while deterministic code executes approved plans across several turns.

The current event system sometimes includes the current turn in dedupe keys for strategic questions. An unresolved question can therefore appear under a new identity on the next turn and become eligible for another planning call.

The current plan model also lacks an explicit lease, revision, and invalidation policy. The runtime cannot state precisely why an old plan remains valid or why a new planner call is necessary.

## Decision

### Stable decision identity

A strategic question receives a semantic identity independent of observation time unless the question is inherently turn-specific.

Examples:

```text
settler-site-selection:unit-7
unit-plan-review:unit-12:plan-revision-4
research-direction:empire:strategy-revision-3
city-role:city-2:expansion-phase-1
```

The following belong in decision metadata or the decision input hash, not normally in the stable identity:

- current turn;
- observation ID;
- last-seen timestamp;
- current provider request ID.

### Decision input hash

Each planner-eligible decision gap records a deterministic hash of the information that materially affects the answer:

```text
gap type and scope
+ relevant observation projection
+ relevant plan revisions
+ current strategy revision
+ policy/configuration revision
```

Repeated eligibility checks with the same stable identity and input hash do not create a new logical planner request before the configured retry/review policy allows it.

### Plan leases

A planner result creates one or more revisioned, scope-specific plan leases.

A lease contains:

```text
scope
subject IDs
plan revision
valid-from turn
valid-until turn or completion condition
invalidation conditions
review conditions
continuation policy
approval state
```

A plan remains authoritative for deterministic continuation while:

- its lease is active;
- its preconditions remain true;
- no invalidation condition is triggered;
- no higher-priority safety condition intervenes.

A review date does not automatically require a planner call. The planner eligibility gate may extend the lease when material inputs are unchanged and policy allows extension.

## Planner eligibility rule

A logical planner request is allowed only when all are true:

1. a first-class strategic decision gap is open;
2. no valid deterministic rule or active plan resolves it;
3. the gap is not a system error, approval wait, verification state, or uncertain mutation;
4. the stable identity/input-hash pair has not already been resolved or recently attempted under policy;
5. the turn and runtime budgets allow a request;
6. required deterministic information queries have completed or are part of one bounded planning transaction.

## Consequences

### Positive

- ordinary planned turns make zero planner calls;
- persistent questions do not become new questions merely because the turn changed;
- multi-turn unit, city, research, and civic plans continue deterministically;
- planner calls become auditable by reason;
- partial invalidation can replan one scope without replacing all plans.

### Negative

- event identity and decision identity must be modeled separately;
- plan validity needs explicit evaluators;
- relevant-state projection and hashing become versioned contracts;
- a bad invalidation policy can keep an obsolete plan too long, so safety conditions must remain conservative.

## Turn-specific exceptions

Turn may be part of identity only when the decision itself expires with that turn, for example:

- whether to take a one-turn tactical attack opportunity;
- a trade offer that has a unique upstream offer ID and turn expiry;
- a World Congress vote instance;
- a temporary emergency response window.

The event/gap type must declare this policy. Developers must not add the turn merely to avoid dedupe collisions.

## Acceptance tests

- the same unresolved settler question across turns retains one stable gap identity;
- unchanged relevant input creates no second logical planner request;
- changed target availability changes the input hash and may reopen eligibility;
- an active research plan suppresses planner calls until invalidated or completed;
- invalidating one city plan does not invalidate unrelated plans;
- routine unit orders never create strategic gaps;
- logical planner requests and physical provider attempts are counted separately;
- a planner-requested information round trip remains one logical request.
