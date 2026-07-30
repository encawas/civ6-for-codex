## Purpose

Defines the exclusive research write-authority cutover and the ordering rules that let normal routing consume only an activated StrategicContract revision.

## ADDED Requirements

### Requirement: Research authority switches atomically

Approval activation SHALL add research to the MissionGraph-owned AuthorityScopeSet in the same transaction that commits the Proposal-derived StrategicContract revision. No committed state SHALL expose both legacy and MissionGraph research writers or a research Mission without MissionGraph authority.

#### Scenario: Research activation succeeds

- **WHEN** an eligible research Proposal is approved
- **THEN** the new Contract revision and research AuthorityScopeSet ownership become visible together

#### Scenario: Authority write fails

- **WHEN** the research authority update cannot be persisted
- **THEN** Approval, Contract revision, Applied Tick, Runtime transition, and wait clearance all roll back

#### Scenario: Mission write fails

- **WHEN** the proposed research Mission cannot be persisted with the new revision
- **THEN** research remains legacy-owned and no activation evidence commits

### Requirement: Legacy research writes stop after cutover

After research becomes MissionGraph-owned, every legacy path that could create or update research DecisionGap, PlanLease, strategy state, or equivalent research intent SHALL reject or remain read-only. Historical audit MAY remain readable.

#### Scenario: Legacy research write after activation

- **WHEN** a legacy path attempts a new research strategic write after the authority switch
- **THEN** the write is rejected before it can become workflow authority

#### Scenario: Historical legacy research read

- **WHEN** audit or replay reads legacy research history after activation
- **THEN** the history remains available but cannot create claimable or authoritative work

#### Scenario: No dual write during deployment

- **WHEN** an application version that lacks MissionGraph research authority encounters an already switched game
- **THEN** it fails closed rather than resume legacy research writes

### Requirement: Legacy research execution is quiescent at activation

While holding the BEGIN IMMEDIATE writer lock and before committing the research AuthorityScopeSet transfer, approval activation SHALL re-read every legacy research StoredTask, all associated ActionAttempts, and any pending task-confirmation state. The same transaction SHALL leave no claimable, in-flight, verifying, uncertain, or otherwise revivable legacy research work. If quiescence cannot be proved, the entire activation SHALL roll back.

#### Scenario: Safely disposable work is cancelled atomically

- **WHEN** legacy research tasks are PENDING, READY, BLOCKED, FAILED, ESCALATED, or AWAITING_CONFIRMATION when approval holds the writer lock
- **THEN** the activation transaction moves them to the permanently non-revivable CANCELLED state, records their previous state and authority-switch reason in activation audit, and closes any legacy confirmation without treating it as Contract approval

#### Scenario: In-flight or unresolved work blocks activation

- **WHEN** a legacy research task is RUNNING, VERIFYING, or UNCERTAIN, or any associated ActionAttempt is PREPARED, VERIFYING, or UNCERTAIN
- **THEN** activation rolls back before Approval, Contract, MissionGraph authority, Runtime, or wait state changes

#### Scenario: Permanent terminal history remains unchanged

- **WHEN** a legacy research task is DONE, CANCELLED, or EXPIRED and has no unresolved ActionAttempt
- **THEN** activation preserves that task and its execution evidence as inert history

#### Scenario: Task appears after preflight

- **WHEN** preflight found no claimable task but legacy routing commits a READY research task before approval acquires BEGIN IMMEDIATE
- **THEN** the locked re-read observes and cancels that task in the activation transaction or blocks activation

#### Scenario: Forged switched history retains legacy execution

- **WHEN** startup or replay contains MissionGraph research authority together with claimable, in-flight, verifying, uncertain, or revivable legacy research work
- **THEN** aggregate validation fails closed and replay rejects the import before deleting target data

#### Scenario: Quiescence creates no replacement task

- **WHEN** the activation transaction disposes legacy research execution
- **THEN** it creates no Mission-derived StoredTask and later Routing remains the only path to a new revision-bound Planner lifecycle

### Requirement: Non-research scopes remain unchanged

The research activation transaction SHALL NOT transfer or modify the write authority, Missions, legacy plans, or execution work of any non-research scope.

#### Scenario: Civic remains legacy-owned

- **WHEN** research activation commits while civic is legacy-owned
- **THEN** civic remains legacy-owned and its strategic state is unchanged

#### Scenario: Strategic writes respect scope ownership

- **WHEN** research is MissionGraph-owned and civic is legacy-owned
- **THEN** MissionGraph-owned research strategic writes may pass scope validation and writes targeting legacy-owned non-research scopes are rejected

### Requirement: Dedicated decision transition precedes ordinary work

The decision or activation Tick SHALL complete before any ordinary Routing, Observing, planning, or execution Tick begins. No other Tick MAY overlap the decision transition, and ordinary work SHALL NOT observe a partial decision aggregate.

#### Scenario: Ordinary Tick follows activation

- **WHEN** approval activation commits and its Applied Tick completes
- **THEN** Runtime enters ROUTING and the next ordinary Tick starts no earlier than the Applied Tick completion

#### Scenario: Ordinary Tick overlaps activation

- **WHEN** startup or replay contains an ordinary Tick that starts before the decision or activation Tick completes
- **THEN** validation fails closed

#### Scenario: Ordinary Tick precedes decision evidence

- **WHEN** history claims ordinary work continued after user approval but before a dedicated decision Tick
- **THEN** startup and replay reject the impossible ordering

#### Scenario: Rejection transition precedes routing

- **WHEN** a Proposal is rejected
- **THEN** its Rejected Tick completes before ordinary routing resumes

#### Scenario: Invalidation transition precedes routing

- **WHEN** a Proposal is invalidated
- **THEN** its Invalidated Tick completes before ordinary routing resumes

### Requirement: Routing consumes active authoritative revisions

After cutover, research routing SHALL read the active StrategicContract revision and its authoritative MissionGraph. Work derived from a stale Contract or Mission revision SHALL NOT become claimable.

#### Scenario: Current revision is routed

- **WHEN** Runtime routes research after activation
- **THEN** it uses the active Contract revision and research Mission recorded by the approval transaction

#### Scenario: Stale Mission revision cannot create work

- **WHEN** a projection references a Contract or Mission revision that is no longer active
- **THEN** the projected work is rejected or made unclaimable before execution

#### Scenario: Restart resumes authoritative routing

- **WHEN** the process restarts after research activation
- **THEN** routing derives research ownership and content from the persisted active Contract and AuthorityScopeSet

### Requirement: StoredTask projection has durable Mission provenance

The existing StoredTask and workflow_tasks representation SHALL gain the following optional, grouped source fields without introducing a second task model or table:

```text
source_contract_id
source_contract_revision
source_mission_id
source_mission_revision
```

The four fields SHALL be either all null or all present. PR 1C-1 SHALL add the domain contract, nullable SQLite columns, all-null legacy migration, canonical serialization, replay import/export, and ordinary-save/startup/replay validation. It SHALL NOT change current research routing. A Mission-derived research StoredTask SHALL carry all four fields and they SHALL identify the active Contract revision and active research Mission revision for the same game.

#### Scenario: Existing task remains explicitly legacy

- **WHEN** an existing or migrated StoredTask has action_type set_research and all four source fields are null before the first research authority cutover
- **THEN** it is classified as a legacy research StoredTask rather than being assigned inferred provenance

#### Scenario: Mission-derived research task has complete provenance

- **WHEN** post-activation Routing projects a set_research task from the active research Mission
- **THEN** all four source fields are present and match the active Contract and Mission identities and revisions

#### Scenario: Partial provenance is rejected

- **WHEN** ordinary persistence, startup, or replay observes only part of the four-field provenance group
- **THEN** validation fails closed and replay rejects before deleting target data

#### Scenario: Legacy task cannot become claimable after cutover

- **WHEN** research is MissionGraph-owned and a set_research task has null provenance
- **THEN** claim, retry, confirmation release, and recovery reject it as legacy execution

#### Scenario: Stale projected task cannot become claimable

- **WHEN** claim, retry, confirmation release, or recovery sees a Mission-derived research task whose source Contract or Mission identity or revision differs from the active aggregate
- **THEN** that write transaction fails closed or makes the task permanently unclaimable before execution

#### Scenario: Current projected task is eligible

- **WHEN** a Mission-derived set_research task has complete provenance matching the active Contract and Mission revision
- **THEN** the normal task lifecycle may evaluate it without reopening a legacy research write path

### Requirement: Proposal application does not create StoredTask

The decision and activation transaction SHALL NOT directly create a StoredTask. Executable work SHALL be produced only by a later deterministic projection from the active Contract and authoritative MissionGraph through the normal Planner and execution lifecycle.

#### Scenario: Approval commits no StoredTask

- **WHEN** a Proposal is approved
- **THEN** no StoredTask is created in the approval transaction

#### Scenario: Routing projects later work

- **WHEN** a later routing step consumes the active research Mission
- **THEN** any planner work is revision-bound and enters the normal PlannerRequest lifecycle before a StoredTask can exist

#### Scenario: Proposal identity alone cannot create work

- **WHEN** a caller presents only a Proposal or Applied Tick without the active matching Contract revision
- **THEN** no claimable task is produced

### Requirement: Activation remains dormant until final enablement

Protocol and dormant activation components MAY be deployed before user entry and Engine integration, but no production path SHALL invoke research decision or authority activation until PR 1C-3 explicitly enables it.

#### Scenario: PR 1C-1 deployment

- **WHEN** protocol and persistence foundations are deployed
- **THEN** existing Phase 1B Proposal waits and legacy research authority behave unchanged

#### Scenario: PR 1C-2 deployment

- **WHEN** atomic activation and routing projection are present behind a dormant gate
- **THEN** production users cannot invoke them and legacy research remains authoritative

#### Scenario: PR 1C-3 enablement

- **WHEN** the user entry point, Engine integration, and end-to-end gates pass
- **THEN** research decision and activation may be enabled without changing non-research scopes
