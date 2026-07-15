# Planner Call Policy

## 1. Goal

The planner exists to resolve strategic ambiguity, not to participate in every turn or every tick. Runtime speed depends primarily on how long approved plans and deterministic rules can continue without another model call.

The required default is:

```text
ordinary planned turn: 0 logical planner requests
strategic decision turn: usually 1 logical planner request
```

## 2. Terminology

- **Logical planner request**: one semantic decision request with one stable request ID, even if the provider adapter performs transport retries.
- **Provider attempt**: one HTTP or CLI attempt made to deliver the same logical request.
- **Decision gap**: a current strategic question that valid rules and approved plans cannot resolve.
- **Decision group**: one or more compatible decision gaps bundled against the same observation revision.
- **Plan lease**: the period and conditions under which an approved plan remains valid without another planner call.

Budgets apply to logical planner requests, not low-level provider attempts.

## 3. Hard eligibility gate

The runtime MUST NOT call the planner unless all conditions below hold:

1. At least one unresolved decision gap is explicitly routed to the planner.
2. No higher-priority deterministic or already approved task is ready.
3. No mutation is executing, verifying, uncertain, or transitioning the turn.
4. No mandatory approval or human-only decision is already waiting.
5. Required focused context has been gathered.
6. No valid approved plan already resolves the gap.
7. The event is not in cooldown and the same request was not already answered for the same relevant revisions.
8. The logical request budget permits the call.

A new turn number alone is never sufficient reason to call the planner.

## 4. Cases that MUST NOT call the planner

The following are rule/runtime responsibilities:

- continuing active production, research, or civic selection;
- selecting the next item from an approved queue when the slot is truly empty;
- moving a unit along an approved route while safety conditions remain valid;
- healing, fortifying, skipping, or continuing ordinary unit behavior under configured rules;
- verifying previous actions;
- reconciling stale events or tasks;
- normalizing upstream values such as `none`, `nothing`, empty strings, or null;
- resolving connection, lock, schema, or persistence failures;
- deciding whether an irreversible action was committed;
- waiting for or recording approval;
- ending a turn after all safety conditions are satisfied;
- periodic review when the rule-based review gate finds no material strategic change.

## 5. Cases that MAY justify a planner call

Examples include:

- first-stage opening strategy when no plan exists;
- settler destination selection or material replanning;
- war, peace, major tactical commitment, or emergency defense posture;
- city specialization or district placement with meaningful trade-offs;
- major production, research, civic, government, policy, religion, or victory-path reprioritization;
- a previously approved plan invalidated by a material game change;
- several compatible strategic decisions that should be coordinated together.

The router MUST still prove that deterministic rules and current plans cannot resolve the case.

## 6. Logical request budget

Default policy:

```text
max_logical_planner_requests_per_turn = 1
max_concurrent_logical_requests_per_game = 1
```

A manual user-requested replan MAY override the per-turn budget when explicitly configured, but the override MUST be visible and audited.

Provider retries MUST reuse the same logical request ID and input hash. A retry with changed semantic input is a new logical request and consumes budget.

The planner adapter MAY retry transient transport failures according to provider policy, but it MUST NOT reinterpret, enlarge, or split the decision request during retry.

## 7. Decision batching

Compatible decision gaps SHOULD be batched into one logical request when they:

- share the same observation revision;
- do not require one decision to mutate the game before another can be evaluated;
- benefit from coordinated strategic reasoning;
- fit within context and output budgets;
- can be validated independently or as an explicitly declared atomic group.

Examples of a useful batch:

- next production direction;
- next research direction;
- next civic direction;
- response to a newly met civilization;
- settler destination reconsideration.

The batch MUST exclude runtime failures, verification ambiguity, approval state, and other non-strategic workflow conditions.

Each output item MUST identify the decision gap it resolves. One invalid item SHOULD NOT invalidate unrelated valid items unless the planner declared an atomic dependency.

## 8. Plan leases

Every planner-created plan MUST define a lease containing:

- `valid_from_turn`;
- `valid_until_turn` or a stage-completion condition;
- plan revision;
- covered subjects and slots;
- explicit invalidation conditions;
- optional review hints;
- approval state.

Example:

```text
scope: opening strategy
valid from: turn 1
valid until: second city founded or turn 20
production queue: scout -> slinger -> settler
research direction: mining -> pottery -> writing
invalidation:
  - declaration of war
  - settler target becomes illegal or occupied
  - severe barbarian threat near capital
  - production item becomes unavailable
```

While a lease remains valid, covered routine events MUST NOT trigger a planner call.

## 9. Lease validation

A rule-based lease validator runs before planner eligibility. It evaluates only observable conditions and approved policy.

Possible results:

- `VALID`: continue without planner call;
- `PARTIALLY_VALID`: preserve unaffected plan scopes and create gaps only for invalid scopes;
- `EXPIRED`: create bounded decision gaps;
- `INVALIDATED`: stop dependent tasks and create bounded decision gaps;
- `UNKNOWN`: gather focused information or ask for human review; do not automatically call the planner without context.

Invalidating one city or unit plan MUST NOT automatically invalidate unrelated strategy scopes.

## 10. Periodic strategic review

Periodic review is a rule gate, not an automatic planner call.

Recommended configurable defaults:

```text
early_game_review_interval_turns = 5
mid_game_review_interval_turns = 8
late_game_review_interval_turns = 5
```

At a review boundary, the runtime compares compact strategic indicators such as:

- military threat level;
- city count and expansion progress;
- production/research/civic queue coverage;
- economy and resource pressure;
- newly met civilizations or city-states;
- war and diplomacy changes;
- stage objectives completed or delayed.

If no material change or uncovered decision gap exists, the runtime extends or retains the lease without calling the planner.

## 11. Event cooldown and deduplication

Planner-triggering events MUST have a stable deduplication identity and cooldown policy.

The runtime MUST NOT repeat a logical request when all of the following are unchanged:

- decision-gap identity;
- relevant observation projection hash;
- applicable plan revisions;
- approval state;
- allowed actions and policy revision.

A provider failure MAY be retried under backoff with the same logical request ID. A successful or validly rejected request MUST not be regenerated merely by another tick.

## 12. Focused context contract

The planner receives only the context required for the decision group:

```text
named decision gaps
+ relevant observation projection
+ relevant approved plans and revisions
+ compact strategic state
+ allowed plan/action/condition schema
+ risk and approval policy
+ output budget
```

The full raw snapshot, complete logs, all units, or all historical events MUST NOT be included by default.

When context is missing, the planner MAY request a bounded read-only information query. The runtime executes allowed queries, persists the result, and decides in a later tick whether the logical planner request should be issued or continued.

The planner MUST NOT invent entity IDs, coordinates, legal actions, or current slot state.

## 13. Output contract

Planner output MUST be structured and versioned. Each decision item MUST contain:

- decision-gap ID;
- disposition: propose plan, propose task, request information, defer to human, or no change;
- rationale;
- affected scope and subjects;
- expected outcome;
- validity horizon and invalidation conditions;
- required approval level;
- relevant preconditions and postconditions;
- observation and plan revisions used.

Low-level tasks are proposals until runtime validation succeeds.

## 14. Planner failure behavior

Planner failure MUST NOT automatically block a turn when safe approved plans and deterministic actions remain available.

The runtime SHOULD:

- classify authentication, quota, rate limit, timeout, provider, parsing, and validation failures separately;
- persist logical request and provider-attempt metadata;
- apply cross-tick backoff;
- continue unrelated deterministic work;
- stop only when a mandatory strategic decision is truly unresolved.

The runtime MUST NOT create repeated immediate planner calls in the same turn after a failed logical request.

## 15. Metrics and acceptance targets

At minimum, record:

- logical requests per turn and per game;
- provider attempts per logical request;
- percentage of turns completed with zero planner calls;
- decision gaps by route;
- plan lease duration and invalidation reason;
- prompt/context size;
- response latency and validation failures;
- duplicate-call suppression count;
- rule-resolved events versus planner-resolved events.

Initial acceptance targets for the refactor:

- at least 80% of ordinary non-war turns with zero logical planner requests in stable planned play;
- no more than one automatic logical planner request per turn;
- no planner request caused solely by turn advancement or routine unit orders;
- no repeated successful logical request for unchanged decision inputs;
- planner outage does not disable deterministic execution or state verification.