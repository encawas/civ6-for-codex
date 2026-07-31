# research-authority-routing Specification

## Purpose

Defines the exclusive research write-authority cutover and the ordering rules that let normal routing consume only an activated StrategicContract revision.

## Requirements

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

### Requirement: Legacy research execution is quiescent before retirement

Phase 6 migration SHALL archive and remove legacy execution authority only after every legacy StoredTask is permanently terminal and every associated ActionAttempt is resolved. Current schema databases SHALL contain no workflow_tasks table and no current activation or recovery path SHALL recreate or mutate legacy execution.

#### Scenario: Terminal legacy execution is archived

- **WHEN** a pre-v14 database contains only DONE, CANCELLED, or EXPIRED legacy tasks with no unresolved ActionAttempt
- **THEN** migration writes hash-verified audit archive records and removes the legacy authority tables

#### Scenario: Unresolved legacy execution blocks migration

- **WHEN** a pre-v14 database contains claimable, in-flight, verifying, uncertain, or otherwise recoverable legacy execution
- **THEN** migration fails closed and leaves the pre-v14 database unchanged

#### Scenario: Replay migration fails before target deletion

- **WHEN** a legacy replay contains execution that cannot be safely retired
- **THEN** replay preflight rejects it before deleting any target-game state

#### Scenario: Current runtime cannot reopen legacy execution

- **WHEN** scope activation, graph activation, retry, confirmation, or rewind recovery runs on a current database
- **THEN** it reads and writes only TurnActionGraph execution state and cannot create or mutate a legacy StoredTask

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

### Requirement: TurnActionNode projection has durable Mission provenance

Every TurnActionNode SHALL carry the following source fields:

```text
source_contract_id
source_contract_revision
source_mission_id
source_mission_revision
```

The fields SHALL identify the active Contract revision and the same ACTIVE Mission for the same game. A research node's action_type SHALL be exactly set_research. The action SHALL come from the closed research -> set_research mapping and SHALL NOT be selected from a tool-name string or free JSON. Legacy task provenance remains available only in the immutable migration archive and cannot become execution authority.

#### Scenario: Mission-derived research node has complete provenance

- **WHEN** Routing projects a set_research node from the ACTIVE research Mission bound by the active Contract revision
- **THEN** all four source fields are present and match the active Contract and Mission identities and revisions

#### Scenario: Non-research or non-ACTIVE provenance is rejected

- **WHEN** routing or a node mutation binds civic, production, unit, city, PAUSED, BLOCKED, COMPLETED, FAILED, CANCELLED, INVALIDATED, or any other Mission not exactly ACTIVE research
- **THEN** no Mission-derived research node becomes claimable and non-research authority remains unchanged

#### Scenario: Wrong action semantic is rejected

- **WHEN** provenance is complete but action_type is not set_research, or free JSON attempts to select another tool
- **THEN** routing and every node mutation fail closed

#### Scenario: Partial provenance is rejected

- **WHEN** ordinary persistence, startup, or replay observes missing or mismatched source provenance
- **THEN** validation fails closed and replay rejects before deleting target data

#### Scenario: Archived legacy task cannot become claimable

- **WHEN** historical replay or migration data contains a legacy set_research StoredTask
- **THEN** it remains archive-only and cannot enter current graph claim, retry, confirmation, or recovery

#### Scenario: Stale projected task cannot become claimable

- **WHEN** claim, retry, confirmation release, or recovery sees a research node whose source Contract or Mission identity or revision differs from the active aggregate
- **THEN** that write transaction fails closed or makes the node permanently unclaimable before execution

#### Scenario: Current projected task is eligible

- **WHEN** a set_research node has complete provenance matching the active Contract and Mission revision
- **THEN** the TurnActionGraph lifecycle may evaluate it without reopening a legacy research write path

### Requirement: Proposal application does not create executable nodes

The decision and activation transaction SHALL NOT directly create a TurnActionNode. Executable work SHALL be produced only by a later deterministic projection from the active Contract and authoritative MissionGraph through the current-turn TurnActionGraph/TurnActionNode persistence and execution lifecycle. This projection SHALL NOT create another PlannerRequest or recall the Provider.

#### Scenario: Approval commits no executable node

- **WHEN** a Proposal is approved
- **THEN** no TurnActionNode is created in the approval transaction

#### Scenario: Routing projects later work

- **WHEN** a later routing step consumes the active research Mission
- **THEN** the revision-bound deterministic projection activates a current-turn TurnActionGraph before a TurnActionNode can become claimable, without creating another PlannerRequest or recalling the Provider

#### Scenario: Proposal identity alone cannot create work

- **WHEN** a caller presents only a Proposal or Applied Tick without the active matching Contract revision
- **THEN** no claimable task is produced

### Requirement: Activation uses the dedicated enabled decision path

Production composition SHALL enable research decision and authority activation only through the dedicated APPROVE, REJECT, and system invalidation aggregate transactions. Generic Human Wait resume, ordinary Tick persistence, and direct record saves SHALL NOT substitute for a Proposal decision.

#### Scenario: User approves or rejects

- **WHEN** a user decides an OPEN Proposal through the control surface
- **THEN** the corresponding dedicated aggregate transaction records the sole terminal decision and transition Tick

#### Scenario: Generic resume targets Proposal wait

- **WHEN** generic Human Wait resume is requested for a Proposal-ready wait
- **THEN** it is rejected without changing Proposal, Contract, authority, or Runtime state

#### Scenario: Direct terminal evidence is attempted

- **WHEN** a caller attempts to save ApprovalRecord or a terminal Proposal Tick outside the aggregate transaction
- **THEN** the Store rejects the standalone write
