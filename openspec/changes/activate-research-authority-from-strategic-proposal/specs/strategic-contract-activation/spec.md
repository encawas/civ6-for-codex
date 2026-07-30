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

The new StrategicContract revision SHALL preserve the Proposal's canonical strategic objectives, global constraints, proposed research Mission, target Contract identity, source Observation, and Proposal hash. Approval SHALL NOT edit, regenerate, or rebase that content.

#### Scenario: Approved content is copied canonically

- **WHEN** approval activates a valid Proposal
- **THEN** the new revision's research content is canonically equivalent to the immutable Proposal content

#### Scenario: Edited approval is rejected

- **WHEN** approval supplies replacement objectives, constraints, Mission content, Contract identity, or base revision
- **THEN** activation fails and no terminal fact is persisted

### Requirement: ContractCommit uses structured source binding

Every Proposal-derived StrategicContractCommit SHALL structurally bind the source Proposal ID, source Proposal hash, source Approval ID, source PlannerRequest ID, and expected base revision. A free-form reason SHALL NOT substitute for any binding.

#### Scenario: Complete source binding

- **WHEN** a Proposal-derived revision commits
- **THEN** its ContractCommit contains all structured source identities and they match the persisted evidence

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

The durable APPROVED disposition SHALL consist of the immutable Proposal, matching APPROVED ApprovalRecord, matching StrategicContractCommit, expected StrategicContract revision, matching StrategicProposalAppliedTick with immutable legacy execution disposition audit references, and aggregate proof that no claimable, in-flight, verifying, uncertain, or revivable legacy research execution survived activation. The evidence SHALL exclude any invalidation fact.

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
