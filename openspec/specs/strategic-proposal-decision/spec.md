# strategic-proposal-decision Specification

## Purpose

Defines the durable and mutually exclusive evidence from which a StrategicResearchProposal disposition is derived without mutating the Proposal itself.

## Requirements

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

### Requirement: Phase 1B released waits migrate before decision enablement

A StrategicProposalWaitResumedTick created by the Phase 1B generic resume path SHALL remain interaction evidence only and SHALL NOT be interpreted as approval, rejection, or application. Before PR 1C-3 enables the Proposal decision protocol, the system SHALL atomically classify every historical OPEN Proposal while holding the WorkflowStateStore writer boundary.

An OPEN Proposal with its unresolved strategic_contract_proposal_ready wait SHALL remain OPEN and eligible for APPROVE or REJECT. An OPEN Proposal with a historical StrategicProposalWaitResumedTick and no terminal decision SHALL receive exactly one StrategicProposalInvalidatedTick with reason PRE_PHASE1C_WAIT_RELEASED. If its Contract target or base is already stale, the existing deterministic stale invalidation reason takes precedence. The migration SHALL write no ApprovalRecord, Contract revision, ContractCommit, MissionGraph authority, StoredTask, or Provider call.

A migration-generated StrategicProposalInvalidatedTick SHALL carry typed origin PHASE1C_ENABLEMENT_MIGRATION and structurally bind its ProposalReadyTick, ResumeRequest, and StrategicProposalWaitResumedTick. Its started_at and completed_at SHALL be equal to one microsecond after the maximum of Proposal.created_at, source PlannerRequest.completed_at, final ProviderAttempt.completed_at, ProposalReadyTick.completed_at, ResumeRequest.requested_at, and StrategicProposalWaitResumedTick.completed_at. This is a logical causal time derived from durable Phase 1B evidence, not a reconstructed user-operation time or migration wall clock.

Ordinary persistence, startup, and replay SHALL resolve the same bound records and recompute the exact canonical time. Missing or mismatched source evidence, a timestamp before or unequal to that value, or a non-representable next microsecond SHALL fail closed. Migration failure SHALL preserve the old database. A valid migrated database SHALL reopen and SHALL preserve the same InvalidatedTick through export, empty-store import, and re-export without regenerating its time.

After PR 1C-3 enables the decision protocol, a strategic_contract_proposal_ready wait SHALL leave AWAITING_HUMAN only through the dedicated APPROVE or REJECT aggregate transition. The generic request_human_resume operation SHALL reject that wait while remaining available to non-Proposal waits such as strategic_request_terminated.

#### Scenario: Unresolved Phase 1B wait remains decidable

- **WHEN** enablement migration finds an OPEN Proposal with a valid unresolved Proposal-ready wait and no Resume Tick
- **THEN** the Proposal remains OPEN and the wait remains available to APPROVE or REJECT

#### Scenario: Released Phase 1B wait is invalidated

- **WHEN** enablement migration finds an OPEN Proposal with a StrategicProposalWaitResumedTick and no human or system terminal fact
- **THEN** it atomically records one source-bound StrategicProposalInvalidatedTick with reason PRE_PHASE1C_WAIT_RELEASED and the canonical causal time, without creating approval or activation evidence

#### Scenario: Valid Phase 1B history migrates deterministically

- **WHEN** all required Proposal, Request, Attempt, Ready, ResumeRequest, and WaitResumed evidence is present and causally valid
- **THEN** migration writes the canonical logical time, startup accepts it, and replay round trips the same Tick unchanged

#### Scenario: Forged migration time fails closed

- **WHEN** ordinary save, startup, or replay sees a migration-origin InvalidatedTick whose time precedes or differs from the canonical causal time
- **THEN** validation rejects the aggregate and replay leaves existing target data untouched

#### Scenario: Missing migration source fails closed

- **WHEN** migration cannot resolve every structurally bound causal source or cannot represent the next microsecond
- **THEN** migration writes nothing and the pre-upgrade database remains readable under the pre-enable behavior

#### Scenario: Stale reason takes precedence

- **WHEN** a historical resumed OPEN Proposal also has a stale Contract target or expected base revision
- **THEN** the existing target-stale or base-stale invalidation reason is recorded instead of PRE_PHASE1C_WAIT_RELEASED

#### Scenario: Multiple released Proposals migrate independently

- **WHEN** one game contains multiple historical OPEN Proposals whose waits were resumed before Phase 1C
- **THEN** each Proposal receives its own deterministic idempotent invalidation and no Proposal is treated as approved

#### Scenario: Generic resume cannot release a Proposal wait after enablement

- **WHEN** request_human_resume targets a strategic_contract_proposal_ready wait after the decision protocol is enabled
- **THEN** it fails closed and leaves the Proposal, Runtime, and Human Wait unchanged

#### Scenario: Non-Proposal resume remains available

- **WHEN** request_human_resume targets a supported non-Proposal wait after enablement
- **THEN** the existing non-Proposal resume policy remains unchanged

#### Scenario: Pre-enable replay remains readable

- **WHEN** startup or replay receives a valid Phase 1B OPEN Proposal plus WaitResumedTick before enablement migration completes
- **THEN** it recognizes the state as migration input rather than approval and runs the deterministic migration before enabled ordinary work

#### Scenario: Migrated store has no released OPEN Proposal

- **WHEN** PR 1C-3 enablement migration commits
- **THEN** startup and replay validation reject any remaining OPEN Proposal that already has a StrategicProposalWaitResumedTick

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
