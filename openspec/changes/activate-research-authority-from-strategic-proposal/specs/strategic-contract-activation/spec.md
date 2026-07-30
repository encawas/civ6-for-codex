## Purpose

Defines the all-or-nothing evidence and revision rules that make an approved StrategicResearchProposal effective as one StrategicContract revision.

## ADDED Requirements

### Requirement: Approved activation is atomic

Approval of an eligible OPEN Proposal SHALL atomically persist the durable disposition and quiescence proof for legacy research execution, the APPROVED ApprovalRecord, one StrategicContractCommit, one new StrategicContract revision, one StrategicProposalAppliedTick carrying immutable disposition audit references, the research AuthorityScopeSet transfer, Runtime transition to ROUTING, and Human Wait clearance. No observer SHALL see a committed subset.

#### Scenario: Approval succeeds

- **WHEN** the Proposal evidence, explicit-only wait, ProviderAttempt, source PlannerRequest, target Contract, and expected base revision are valid
- **THEN** all approval and activation facts commit once in one transaction

#### Scenario: Crash after Approval write

- **WHEN** a fault occurs after preparing or writing the ApprovalRecord but before the transaction commits
- **THEN** none of the approval or activation facts become visible after restart

#### Scenario: Crash after Contract write

- **WHEN** a fault occurs after preparing or writing the Contract revision but before the transaction commits
- **THEN** the legacy execution disposition, ApprovalRecord, Contract revision, ContractCommit, authority switch, Applied Tick, Runtime change, and wait clearance all roll back

#### Scenario: Crash before Applied Tick

- **WHEN** a fault occurs after other activation writes but before the Applied Tick is written
- **THEN** the entire transaction rolls back and the Proposal remains eligible for a clean retry

### Requirement: Activation appends the expected revision

The approved revision SHALL equal the Proposal's expected base revision plus one and SHALL be appended to the Proposal's target Contract. Approval SHALL re-read the current Contract head before committing.

#### Scenario: Current base matches

- **WHEN** the Proposal expected base equals the active Contract head
- **THEN** the new Contract revision is exactly expected base plus one

#### Scenario: Base became stale

- **WHEN** the active Contract head changed before approval acquires its transaction
- **THEN** approval writes no human decision or Contract revision and the Proposal follows system invalidation

#### Scenario: Contract creation target now exists

- **WHEN** a creation Proposal expected no Contract but a Contract root now exists
- **THEN** approval writes no Contract and the Proposal is invalidated as stale

### Requirement: Activated content derives from the Proposal

The new StrategicContract revision SHALL preserve the Proposal's canonical strategic objectives, global constraints, proposed research Mission, target Contract identity, source Observation, and Proposal hash. The Proposal and activated revision SHALL contain exactly the same Mission identity and revision with scope research and status ACTIVE. The only executable action semantic SHALL come from the closed mapping research -> set_research. Approval SHALL NOT edit, regenerate, rebase, or use desired_outcome, a tool-name string, or free JSON to select another action.

#### Scenario: Approved content is copied canonically

- **WHEN** approval activates a valid Proposal
- **THEN** the new revision's research content is canonically equivalent to the immutable Proposal content

#### Scenario: Edited approval is rejected

- **WHEN** approval supplies replacement objectives, constraints, Mission content, Contract identity, or base revision
- **THEN** activation fails and no terminal fact is persisted

#### Scenario: Non-research Mission is rejected

- **WHEN** Proposal or activation provenance binds a civic, production, unit, city, or other non-research Mission
- **THEN** the complete activation transaction fails before any Approval, Contract, authority, ContractCommit, or AppliedTick fact commits

#### Scenario: Non-ACTIVE Mission is rejected

- **WHEN** Proposal or activation provenance binds a PAUSED, BLOCKED, COMPLETED, FAILED, CANCELLED, INVALIDATED, or other non-ACTIVE Mission
- **THEN** activation fails atomically and research authority remains legacy-owned

#### Scenario: Free-form action selection is rejected

- **WHEN** desired_outcome, a tool-name string, or free JSON names an operation other than the closed research set_research mapping
- **THEN** validation rejects the aggregate rather than treating the value as executable authority

### Requirement: ContractCommit uses structured source binding

Every Proposal-derived StrategicContractCommit SHALL structurally bind the source Proposal ID, source Proposal hash, source Approval ID, source PlannerRequest ID, expected base revision, source Mission ID, and source Mission revision. The matching StrategicProposalAppliedTick SHALL bind the same Mission identity and revision. Both SHALL resolve to the immutable ACTIVE research Mission copied into the Contract revision. A free-form reason, desired_outcome, or tool-name string SHALL NOT substitute for any binding or action semantic.

#### Scenario: Complete source binding

- **WHEN** a Proposal-derived revision commits
- **THEN** its ContractCommit and AppliedTick contain the same source Mission identity/revision and all structured identities match the Proposal, ACTIVE research Mission, Contract revision, and persisted evidence

#### Scenario: Mission binding mismatch is rejected

- **WHEN** ContractCommit, AppliedTick, Contract revision, or Proposal names a different Mission identity or revision
- **THEN** ordinary persistence, startup, and replay reject the aggregate

#### Scenario: Wrong Proposal hash is rejected

- **WHEN** a ContractCommit binds the correct Proposal ID but a different Proposal hash
- **THEN** startup, replay, and ordinary persistence reject the aggregate

#### Scenario: Reason-only provenance is rejected

- **WHEN** source identities appear only in a descriptive reason field
- **THEN** the ContractCommit is not accepted as Proposal-derived activation evidence

### Requirement: Approval status is a redundant snapshot

A Proposal-derived Contract revision SHALL set approval_status to APPROVED and SHALL have exactly one matching APPROVED ApprovalRecord. The field SHALL NOT independently establish approval. Foundation revisions not produced by a Proposal MAY use NOT_REQUIRED. Effective revisions SHALL NOT use REQUIRED or REJECTED.

#### Scenario: Approved revision has matching Approval

- **WHEN** a Proposal-derived Contract revision is validated
- **THEN** approval_status is APPROVED and the structurally bound APPROVED ApprovalRecord exists

#### Scenario: APPROVED status without Approval is rejected

- **WHEN** a Contract revision has approval_status APPROVED but no matching ApprovalRecord
- **THEN** startup, replay, and ordinary persistence fail closed

#### Scenario: Approval without ContractCommit is rejected

- **WHEN** an APPROVED ApprovalRecord exists without its matching ContractCommit and revision
- **THEN** startup and replay fail closed

### Requirement: Complete approval evidence is required

The durable APPROVED disposition SHALL consist of the immutable Proposal, matching APPROVED ApprovalRecord, matching StrategicContractCommit, expected StrategicContract revision, matching StrategicProposalAppliedTick with immutable legacy execution disposition audit references, the consistently bound Proposal-derived ACTIVE research Mission whose closed action mapping is set_research, and aggregate proof that no claimable, in-flight, verifying, uncertain, or revivable legacy research execution survived activation. The evidence SHALL exclude any invalidation fact or non-research/non-ACTIVE Mission.

#### Scenario: Applied Tick missing

- **WHEN** replay contains Approval, ContractCommit, and Contract revision but omits the Applied Tick
- **THEN** replay fails before changing target data

#### Scenario: ContractCommit missing

- **WHEN** replay contains Approval and an APPROVED Contract revision but omits ContractCommit
- **THEN** replay fails before changing target data

#### Scenario: Approval missing

- **WHEN** replay contains ContractCommit, Contract revision, and Applied Tick but omits Approval
- **THEN** replay fails before changing target data

#### Scenario: Invalidated Proposal has activation evidence

- **WHEN** a Proposal has an invalidation fact together with Approval, ContractCommit, or an activated revision
- **THEN** startup and replay fail closed

### Requirement: Activation recovery is idempotent

Recovery or replay of the same approved decision SHALL resolve to the already committed revision and SHALL NOT append another revision, duplicate authority activation, or call the Provider again.

#### Scenario: Restart after committed approval

- **WHEN** the process restarts after all activation evidence committed
- **THEN** the existing active revision is recovered without incrementing it

#### Scenario: Duplicate approval after uncertain response

- **WHEN** the caller repeats an identical approval because the first response was lost
- **THEN** the system returns the existing ApprovalRecord, revision, and Applied Tick

#### Scenario: Replay round trip

- **WHEN** complete activation history is exported, imported into an empty store, and exported again
- **THEN** the active revision, full revision history, source binding, authority state, and audit evidence remain semantically stable

### Requirement: Replay preflight preserves target data on failure

Replay SHALL validate the complete Proposal decision and Contract activation aggregate before deleting or replacing any target-game state.

#### Scenario: Forged Approval replay

- **WHEN** replay contains an ApprovalRecord that is not bound to a valid persisted Proposal
- **THEN** import fails before deleting existing target-game data

#### Scenario: Forged Contract replay

- **WHEN** replay contains a Proposal-derived Contract revision with incomplete or inconsistent activation evidence
- **THEN** import fails before deleting existing target-game data
