# Domain Contracts

## 1. Purpose

This document defines the canonical meanings and lifecycle constraints for workflow data. Names in the implementation MAY differ temporarily during migration, but their semantics MUST converge on these contracts.

## 2. Game session

A `GameSession` identifies one playable game lineage and MUST include enough information to prevent accidental cross-save execution.

Required identity inputs SHOULD include:

- civilization/player identity;
- save or game identifier when available;
- map/game seed or stable upstream identity when available;
- observed turn;
- runtime-generated session UUID;
- connection/FireTuner endpoint identity.

A detected identity mismatch MUST pause mutation and require explicit resume, reload reconciliation, or new-session creation.

## 3. Observation

An `Observation` is an immutable normalized snapshot or projection of current game facts.

Required fields:

```text
observation_id
game_session_id
turn_number
sequence
observed_at
source_versions
normalization_version
base_state
entity_revisions or projection hashes
```

Normalization MUST occur at the adapter boundary. Core domain logic MUST NOT repeatedly compare upstream spellings such as:

```text
NONE
none
nothing
""
null
```

Instead, it consumes typed canonical values such as `EmptySlot` or `None`.

Observations are append-only audit evidence. Later observations supersede facts but do not rewrite history.

## 4. Event

An `Event` represents a meaningful current condition derived from one or more observations.

Required fields:

```text
event_id
game_session_id
dedupe_key
event_type
subject_type
subject_id
opened_from_observation_id
last_seen_observation_id
status
severity
route
payload
resolved_by_observation_id
resolution_reason
```

Canonical event statuses:

```text
OPEN
RESOLVED
SUPPRESSED
SUPERSEDED
```

Events are not command queues. An event may exist without generating a task.

A repeated current condition updates the existing open event by `dedupe_key`; it MUST NOT create duplicate open events.

If a later observation proves the condition disappeared, the event SHOULD resolve automatically unless an unresolved mutation attempt requires separate reconciliation. Even then, the event status and action-attempt status remain separate.

## 5. Decision gap

A `DecisionGap` is a bounded question that prevents rules and valid plans from determining the next safe behavior.

Required fields:

```text
decision_gap_id
source_event_ids
gap_type
scope
subject_ids
observation_id
relevant_plan_revisions
required_context
route
status
cooldown_key
```

Canonical statuses:

```text
OPEN
CONTEXT_REQUIRED
PLANNER_REQUESTED
PROPOSED
RESOLVED
DEFERRED_TO_HUMAN
CANCELLED
SUPERSEDED
```

A system failure, uncertain commit, or approval wait is not a strategic decision gap and MUST NOT be routed to the planner.

## 6. Plan

A `Plan` expresses durable intent across time.

Required fields:

```text
plan_id
game_session_id
scope
subject_ids
revision
status
source
approval_status
created_from_observation_id
valid_from_turn
valid_until_turn or completion_condition
invalidation_conditions
objective
steps or queue
policy_snapshot
supersedes_plan_id
```

Canonical plan statuses:

```text
PROPOSED
ACTIVE
PAUSED
COMPLETED
INVALIDATED
EXPIRED
REJECTED
CANCELLED
SUPERSEDED
```

Plans are revisioned. Editing an active plan creates a new revision and supersedes the previous revision; audited history MUST remain available.

A plan may cover a strategy, city, unit, builder, research route, civic route, diplomatic stance, or another explicit scope. One invalid scope MUST NOT implicitly invalidate unrelated scopes.

## 7. Task

A `Task` is one concrete candidate operation or bounded local workflow step.

Required fields:

```text
task_id
game_session_id
idempotency_key
task_type
subject_type
subject_id
slot
arguments
source_plan_id
source_plan_revision
source_event_ids
created_from_observation_id
status
priority
earliest_turn
latest_turn
must_complete_before_end_turn
approval_requirement
preconditions
postconditions
```

Canonical task statuses:

```text
PROPOSED
AWAITING_APPROVAL
READY
EXECUTING
VERIFYING
SUCCEEDED
FAILED
UNCERTAIN
CANCELLED
EXPIRED
SUPERSEDED
ESCALATED
```

Only tasks in `READY` may be selected for mutation delivery.

A task that becomes incompatible with current game facts MUST be cancelled, expired, or superseded; it MUST NOT be kept alive through retries merely because it once matched an old observation.

## 8. Task idempotency

The task idempotency key MUST be stable for equivalent intended work. It SHOULD include:

```text
game_session_id
subject_type
subject_id
slot
desired_outcome
source_plan_revision
execution_window identity where relevant
```

Example:

```text
game-123:city:1:production:UNIT_SCOUT:plan-rev-4
```

Before inserting a task, the store MUST detect an equivalent task in any active status:

```text
PROPOSED
AWAITING_APPROVAL
READY
EXECUTING
VERIFYING
UNCERTAIN
```

A previously terminal task MAY permit a new task only if current facts and policy justify a new attempt or repeated future action.

## 9. Action contract

An `ActionContract` defines the runtime behavior for one mutation type.

Required fields or methods:

```text
action_type
tool/port operation
allowed_subject_types
argument schema
precondition evaluators
postcondition evaluators
approval policy
retry classification
verification projection
stabilization/backoff policy
redaction policy
```

Retry classification MUST distinguish:

- `IDEMPOTENT_OR_DEDUPED`: safe to retry using the same idempotency mechanism;
- `SAFE_IF_PROVEN_NOT_SENT`: retry only with evidence delivery did not occur;
- `NEVER_BLIND_RETRY`: reconcile state or ask a human after unknown delivery.

Irreversible resource expenditure, city founding, builder-charge consumption, diplomacy acceptance, envoy use, purchases, and similar operations SHOULD default to `NEVER_BLIND_RETRY`.

## 10. Action attempt

An `ActionAttempt` records exactly one delivery attempt.

Required fields:

```text
action_attempt_id
task_id
attempt_number
request_id
idempotency_key
prepared_from_observation_id
prepared_at
sent_at
response_received_at
status
normalized_arguments
transport_result
tool_result
verification_status
last_verification_observation_id
parent_attempt_id
```

Canonical statuses:

```text
PREPARED
REJECTED_BEFORE_SEND
FAILED
VERIFYING
UNCERTAIN
SUCCEEDED
```

The database record MUST exist before external mutation delivery. Runtime restart MUST be able to determine that delivery may have occurred.

## 11. Preconditions and postconditions

Conditions MUST be typed, versioned, auditable, and evaluated by code.

Each condition SHOULD contain:

```text
condition_type
subject reference
parameters
expected result
condition schema version
```

Examples:

- city exists;
- production slot is empty;
- current research is unchanged;
- unit exists and type contains `SETTLER`;
- unit is at coordinate;
- target tile remains legal;
- owned city count is at least a threshold;
- unit is absent;
- turn number is greater than a recorded value.

Prompt text alone is not an enforceable condition.

Unknown condition types MUST fail validation rather than being ignored.

## 12. Approval

An `ApprovalRecord` MUST be immutable and revision-aware.

Required fields:

```text
approval_id
proposal_type
proposal_id
proposal_revision
decision
actor
created_at
reason
edited_payload or replacement_revision reference
```

Canonical decisions:

```text
APPROVED
REJECTED
CANCELLED
REQUESTED_REPLAN
EDITED_AND_APPROVED
```

Approval of one plan revision MUST NOT authorize later revised content automatically.

## 13. Planner request

A `PlannerRequest` is an immutable logical request record.

Required fields:

```text
planner_request_id
game_session_id
turn_number
observation_id
decision_gap_ids
input_projection_hash
plan_revision_refs
policy_revision
model/backend settings
status
created_at
completed_at
response_hash
validation_result
```

Provider attempts are child records and MUST reuse the logical request identity during transport retry.

Equivalent successful logical requests MUST be deduplicated using the relevant input and revision hashes.

## 14. Workflow run and tick outcome

A `WorkflowTick` MUST record:

```text
tick_id
game_session_id
starting_runtime_state
ending_runtime_state
observation_ids
selected_operation
mutation_budget_used
planner_request_id
action_attempt_id
blocking_reason
started_at
completed_at
metrics
```

The tick outcome SHOULD be a closed union such as:

```text
ObservedOnly
ContextGathered
PlanRequested
TaskCreated
MutationSent
AwaitingVerification
AwaitingApproval
AwaitingHuman
PlannerBackoff
TurnTransitionStarted
Paused
SystemError
NoSafeAction
```

A closed outcome type makes multiple accidental operations harder to represent.

## 15. Slot ownership

Tasks that affect mutually exclusive game choices MUST name a canonical slot, for example:

```text
city:{city_id}:production
player:research
player:civic
unit:{unit_id}:order
builder:{unit_id}:charge
player:diplomacy:{other_player_id}:response
```

At most one active task may own a slot unless the task types explicitly support a queue relationship.

A queue plan is not slot ownership. Only the materialized next task owns the current slot.

## 16. Optimistic concurrency

Plans, tasks, approvals, and mutable runtime state MUST use revision or compare-and-swap semantics.

A write based on stale revision data MUST fail explicitly. The runtime then re-observes or reloads; it MUST NOT silently overwrite newer state.

## 17. Schema migration

Persistence changes MUST include:

- an explicit schema version;
- forward migration;
- rollback or documented non-reversibility;
- migration tests from the current production schema;
- restart/recovery tests using migrated data;
- cleanup phase for obsolete columns and shadow tables.

During migration, compatibility reads MAY exist. Dual writes SHOULD be time-bounded and verified; permanent dual-source authority is forbidden.

## 18. Retention and replay

Stable strategic plans, approvals, action attempts, verification evidence, and planner requests SHOULD be retained for replay and audit.

Raw every-turn snapshots MAY be compacted according to policy, but compaction MUST preserve evidence needed to explain:

- why a mutation was allowed;
- what was sent;
- whether it was verified;
- why a plan was invalidated;
- why the planner was or was not called;
- why end turn was permitted.

## 19. Forbidden ambiguous representations

The final architecture MUST NOT rely on:

- one status enum shared by events, plans, tasks, and attempts;
- generic JSON blobs without schema versions for core lifecycle data;
- “success” meaning both tool acceptance and verified game-state change;
- `None` meaning simultaneously unknown, absent, empty, or not loaded;
- current state stored only as mutable singleton rows with no revision evidence;
- task retries that overwrite previous attempt history;
- approval booleans without proposal revision identity.