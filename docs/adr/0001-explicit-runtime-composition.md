# ADR 0001: Explicit Runtime Composition

- Status: Accepted
- Date: 2026-07-15
- Scope: runtime bootstrap, imports, registries, adapters

## Context

The current package assembles effective behavior by importing base modules and then replacing classes, models, registries, prompts, ports, replay types, and web types from `civ6_workflow.__init__`.

This allowed safety fixes to be added quickly without destructive rewrites, but it now creates several problems:

- import order determines behavior;
- source declarations do not reveal effective runtime types;
- tests may unknowingly exercise patched implementations;
- static analysis and refactoring tools see conflicting authorities;
- new fixes tend to create another subclass or `safe_*` layer;
- deleting a legacy module is risky because hidden replacement dependencies are hard to trace.

## Decision

The refactored runtime will use one explicit composition root, provisionally:

```text
src/civ6_workflow/bootstrap.py
```

The composition root is the only place that may select concrete implementations for ports and application services.

It will construct one explicit object graph:

```text
configuration
→ persistence adapter
→ game adapter
→ planner adapter
→ canonical registries
→ application policies
→ bounded Tick runner
→ web/control adapter
```

Domain and application modules must not mutate imported modules or registries.

Public package imports may re-export canonical types, but they must not change the identity or behavior of previously imported objects.

## Implemented composition

Issue #8 establishes `bootstrap.py` as the only production composition root. It
exports the following construction paths:

| API | Responsibility |
|---|---|
| `compose_runtime` | Build the canonical Engine from injected Store, GamePort, Planner, configuration, clock, and crash boundary. |
| `compose_live_runtime` | Select the concrete SQLite, Civ6 MCP/HTTP, and planner adapters for an already-open live session. |
| `open_live_runtime` | Own one live MCP and state-API resource lifetime. |
| `compose_recording_runtime` | Wrap the same live graph with recording adapters. |
| `compose_replay_runtime` | Restore replay state and build the same Engine with replay adapters. |
| `compose_control_panel` | Build the control-plane state and HTTP adapter over the canonical Store. |

The CLI, long-running CLI loop, recording command, replay command, and control
panel all delegate construction to these APIs. Their existing resource lifetime
semantics remain explicit: the long-running CLI reuses one live session, while a
single Tick and a control-panel Tick open and close one session.

The dependency direction is:

```text
CLI / control panel / replay entry points
-> bootstrap composition root
-> concrete adapters (SQLite, MCP/HTTP, planner, web)
-> application Engine and policies
-> domain contracts
```

Domain modules do not import concrete adapters. Application services receive
GamePort and Planner protocols plus the injected Store boundary; adapter
selection is confined to bootstrap.

## Canonical implementations

The retained behavior from the former shadow layers now lives in these modules:

| Former layer | Canonical destination |
|---|---|
| `safe_engine`, `workflow_engine`, `runtime_safety` | `engine.WorkflowEngine` and `EngineConfig` |
| `safe_rules`, `settler_rules` | `rules.DeterministicRuleCompiler` |
| `workflow_conditions` | `conditions.ConditionEvaluator` |
| `safe_store` | `store.WorkflowStore` |
| `safe_mcp_port` | `mcp_port.Civ6GamePort` |
| `safe_replay` | `replay.RecordingGamePort` and `ReplayGamePort` |
| `safe_web_ui` | `web_ui.ControlPanelState`, handler, server, and HTML |
| package registry/model/prompt replacement | definitions or explicit imports in `actions`, `validation`, `workflow_protocol`, and `codex_planner` |

Those shadow modules are deleted. The package initializer is a passive public
re-export and performs no module assignment or registry mutation.

## Preserved semantics

This decision changes composition and source ownership only. It does not change
the SQLite user version, game action contracts, planner-call policy, approval
policy, retry classification, one-mutation Tick budget, or fresh-observation
verification boundary.
## Consequences

### Positive

- the active implementation is inspectable;
- dependency direction becomes enforceable;
- tests can construct the same object graph explicitly;
- old patch layers can be removed incrementally;
- configuration and adapter selection are localized;
- hidden import-order bugs disappear.

### Negative

- callers that relied on import-time replacement need compatibility adapters;
- bootstrap construction becomes a tested API surface;
- migration temporarily requires mapping legacy types to canonical types.

## Migration constraints

1. Add characterization tests for every current replacement before removing it.
2. Do not move all logic at once.
3. Introduce the bootstrap alongside the current path.
4. Route one executable entry point through bootstrap.
5. Compare behavior through replay and contract tests.
6. Remove one replacement only when no caller depends on its import side effect.
7. Do not add any new import-time replacement during migration.

## Rejected alternatives

### Keep the monkey patches but document them

Rejected because documentation cannot make import order structurally safe.

### Create one final `safe_final_*` layer

Rejected because it continues the same authority problem.

### Put dependency selection in the web server or CLI

Rejected because runtime construction must be shared by CLI, web, tests, replay, and future runners.

## Acceptance tests

- importing domain modules does not change classes in other modules;
- action and condition registries have one explicit construction path;
- CLI, web, tests, and replay use the same bootstrap builder;
- effective runtime class identity does not depend on whether `civ6_workflow` or a submodule was imported first;
- no production module name begins with `safe_` after the final removal phase.
