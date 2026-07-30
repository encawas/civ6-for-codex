## Purpose

Defines the durable and mutually exclusive evidence from which a StrategicResearchProposal disposition is derived without mutating the Proposal itself.

## ADDED Requirements

### Requirement: Proposal content remains immutable

The system SHALL preserve the complete canonical StrategicResearchProposal content and hash after first persistence. Terminal handling SHALL NOT add or mutate a Proposal status, approval, rejection, invalidation, replacement, or application field.

#### Scenario: Open Proposal has no embedded status

- **WHEN** a Proposal has no human decision record and no system invalidation fact
- **THEN** its disposition is derived as OPEN without changing the Proposal

#### Scenario: Proposal mutation is rejected

- **WHEN** a caller attempts to replace any persisted Proposal content while recording a terminal disposition
- **THEN** the operation fails closed and the original Proposal remains unchanged

### Requirement: Human decision has one durable authority

The system SHALL derive a human terminal disposition only from one immutable ApprovalRecord bound to the Proposal. The StrategicResearchProposal decision protocol SHALL accept only APPROVED and REJECTED, even if a shared approval type supports other decisions for other workflows.

#### Scenario: Human approves a Proposal

- **WHEN** an eligible user records APPROVED for an OPEN Proposal
- **THEN** the Proposal disposition is derived as APPROVED from the persisted ApprovalRecord

#### Scenario: Human rejects a Proposal

- **WHEN** an eligible user records REJECTED for an OPEN Proposal
- **THEN** the Proposal disposition is derived as REJECTED from the persisted ApprovalRecord

#### Scenario: Unsupported human decision is rejected

- **WHEN** a StrategicResearchProposal decision uses any value other than APPROVED or REJECTED
- **THEN** no terminal fact is persisted and the Proposal remains OPEN

#### Scenario: Transition Tick cannot impersonate approval

- **WHEN** an Applied, Rejected, Resume, Runtime, or Human Wait fact exists without a matching ApprovalRecord
- **THEN** the system does not derive an APPROVED or REJECTED human disposition

### Requirement: System invalidation has one durable authority

The system SHALL derive INVALIDATED only from one immutable StrategicProposalInvalidatedTick bound to the Proposal. A stale Proposal SHALL be invalidated as a system fact and SHALL NOT be represented as a human rejection.

#### Scenario: Base revision changed

- **WHEN** an OPEN Proposal's expected base revision no longer matches the current Contract head
- **THEN** the system atomically records one StrategicProposalInvalidatedTick with a stale-base reason and derives INVALIDATED

#### Scenario: Target Contract changed

- **WHEN** an OPEN Proposal targets a Contract identity that is no longer current
- **THEN** the system records a system invalidation and does not write an ApprovalRecord

#### Scenario: Duplicate invalidation

- **WHEN** the identical invalidation is retried for an already INVALIDATED Proposal
- **THEN** the system returns the existing invalidation fact without adding another Tick

### Requirement: Human and system terminal dispositions are mutually exclusive

Each Proposal SHALL have at most one human terminal ApprovalRecord or one system invalidation fact, never both. APPROVED and REJECTED SHALL also be mutually exclusive.

#### Scenario: APPROVED plus INVALIDATED is rejected

- **WHEN** startup, replay, or a write transaction observes both an APPROVED ApprovalRecord and an invalidation fact for one Proposal
- **THEN** validation fails closed

#### Scenario: REJECTED plus INVALIDATED is rejected

- **WHEN** startup, replay, or a write transaction observes both a REJECTED ApprovalRecord and an invalidation fact for one Proposal
- **THEN** validation fails closed

#### Scenario: APPROVED plus REJECTED is rejected

- **WHEN** two different human terminal ApprovalRecords exist for one Proposal
- **THEN** validation fails closed

#### Scenario: Multiple invalidation facts are rejected

- **WHEN** more than one StrategicProposalInvalidatedTick exists for one Proposal
- **THEN** validation fails closed

### Requirement: Repeated and conflicting decisions are deterministic

An identical repeated decision SHALL be idempotent. A different decision after any terminal disposition SHALL fail as a conflict. Concurrent decision attempts SHALL leave exactly one terminal disposition selected by the first committed transaction.

#### Scenario: Identical approval is repeated

- **WHEN** APPROVED is submitted again with the same immutable identity after approval committed
- **THEN** the existing ApprovalRecord and activation result are returned without another terminal fact

#### Scenario: Identical rejection is repeated

- **WHEN** REJECTED is submitted again with the same immutable identity after rejection committed
- **THEN** the existing ApprovalRecord and Rejected Tick are returned without another terminal fact

#### Scenario: Approval conflicts with rejection

- **WHEN** REJECTED is submitted after APPROVED or APPROVED is submitted after REJECTED
- **THEN** the later operation fails as a terminal-decision conflict

#### Scenario: Two approvals race

- **WHEN** two approval transactions concurrently target the same OPEN Proposal
- **THEN** exactly one ApprovalRecord and one activation result commit and the other transaction returns the same result or an idempotent equivalent

#### Scenario: Approval and rejection race

- **WHEN** approval and rejection transactions concurrently target the same OPEN Proposal
- **THEN** the first committed decision becomes terminal and the other transaction fails as a conflict

### Requirement: Non-approved dispositions cannot activate strategy

REJECTED and INVALIDATED Proposals SHALL NOT create a StrategicContract revision, a ContractCommit, or research MissionGraph authority.

#### Scenario: Rejection creates no Contract revision

- **WHEN** a Proposal is rejected
- **THEN** the active Contract revision and AuthorityScopeSet remain unchanged

#### Scenario: Invalidation creates no Contract revision

- **WHEN** a Proposal is invalidated because its base or target is stale
- **THEN** the active Contract revision and AuthorityScopeSet remain unchanged
