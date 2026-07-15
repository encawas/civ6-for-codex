# Refactor Progress

This file records completed migration slices against the normative refactor
contracts. It describes implemented code, not aspirational architecture.

## Slice 1: canonical domain contracts

Status: complete

Implemented:

- strict frozen models for observations, events, decision gaps, plans, tasks,
  action attempts, approvals, logical planner requests, and workflow Ticks;
- separate lifecycle enums for each domain concept;
- adapter-boundary slot normalization;
- stable task idempotency keys derived from semantic work identity;
- active slot conflict detection across overlapping execution windows;
- structural Tick mutation accounting limited to zero or one;
- lifecycle validation for task approval, action delivery, planner completion,
  event resolution, plan validity, and Tick outcomes.

Characterization catalog coverage:

- `OBS-001`: implemented and passing;
- `OBS-002`: implemented and passing;
- `OBS-006`: implemented and passing;
- `TASK-001`: implemented and passing at the domain identity boundary;
- `TASK-002`: implemented and passing at the conflict boundary;
- `TASK-003`: implemented and passing at the slot ownership boundary.

Invariant impact:

- Persistence: no schema or write path changed.
- Idempotency: canonical semantic identity is now defined, but the legacy store
  does not use it yet.
- Planner calls: no eligibility or budget behavior changed.
- Approval: canonical revision-aware records are defined, but not persisted yet.
- Mutation safety: action-attempt and Tick constraints are representable and
  validated, but the legacy engine is not routed through them yet.
- End turn: no runtime behavior changed.
- Rollback: remove `civ6_workflow.domain` and its tests; no stored data requires
  migration.

Next slice:

1. add the canonical port protocols and explicit bootstrap composition;
2. translate legacy observations into canonical observations;
3. route normalization through that adapter;
4. preserve legacy execution while adding comparison tests;
5. cut over one read-only observation boundary before changing mutation flow.
