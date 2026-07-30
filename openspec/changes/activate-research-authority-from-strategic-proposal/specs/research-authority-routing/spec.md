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

### Requirement: Non-research scopes remain unchanged

The research activation transaction SHALL NOT transfer or modify the write authority, Missions, legacy plans, or execution work of any non-research scope.

#### Scenario: Civic remains legacy-owned

- **WHEN** research activation commits while civic is legacy-owned
- **THEN** civic remains legacy-owned and its strategic state is unchanged

#### Scenario: Research Patch and civic Patch

- **WHEN** research is MissionGraph-owned and civic is legacy-owned
- **THEN** a research Patch may pass scope validation and a civic Patch is deterministically rejected

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

### Requirement: Rollback preserves one research authority

Rollback after enablement SHALL reconcile active attempts and approvals, prove a compatible legacy research baseline, and atomically return research authority without enabling both writers.

#### Scenario: Rollback is blocked by uncertain mutation

- **WHEN** an unresolved research ActionAttempt could duplicate a mutation
- **THEN** rollback remains blocked until observed or human reconciliation completes

#### Scenario: Compatible rollback commits

- **WHEN** active work is reconciled and a compatible legacy baseline is proven
- **THEN** research authority returns atomically to legacy and MissionGraph research writes stop
