# Frontend-led workflow agent architecture

> **Status:** This document remains the high-level architecture overview. During the current refactor, the normative runtime constraints are defined by `REFACTOR_CONSTITUTION.md`, `RUNTIME_STATE_MACHINE.md`, `PLANNER_CALL_POLICY.md`, and `DOMAIN_CONTRACTS.md`. Where this overview appears to describe a whole-turn monolithic cycle, the bounded state-machine documents take precedence.

## Objective

Build a Civilization VI workflow agent whose primary interface is the local control panel. The frontend supervises and initiates work through a local orchestration API; Codex or another model is a replaceable planning service behind that API, never the process that launches or owns the frontend.

The target is not a monolithic agent loop. It is a durable workflow runtime that executes deterministic work locally and calls a planner only for decisions that cannot be resolved safely by rules or approved plans.

## Architectural principles

1. **Frontend is the control plane**
   - The user opens the control panel and explicitly connects, runs, pauses, approves, rejects, or edits plans.
   - The browser calls a localhost API. It never receives API keys or talks directly to the model provider.

2. **Local backend owns orchestration and secrets**
   - API credentials remain in environment variables or a local secret store.
   - The backend exposes narrow planner status, probe, plan, approval, and workflow endpoints.

3. **Deterministic execution comes first**
   - Existing plans, queues, retries, ordinary unit orders, verification, and end-turn safety are resolved without a model.
   - Planner calls are reserved for strategic or ambiguous events.

4. **Planner is an adapter, not the runtime**
   - `ResponsesPlanner`, `CodexCliPlanner`, or future local models implement the same `Planner` interface.
   - The workflow engine does not depend on Codex process state, global configuration, plugins, or MCP.

5. **Every mutation is auditable**
   - Proposed tasks, approval state, actual tool arguments, results, postcondition checks, retries, and planner request IDs are persisted.

6. **Workflow state is durable**
   - Process restarts, game reloads, and temporary planner failures do not lose task or event state.

## Target components

### 1. Browser control plane

Responsibilities:

- connect and test the configured planner;
- show game, MCP, state API, and planner health independently;
- run one workflow cycle or start/pause continuous operation;
- approve, reject, edit, or cancel proposed tasks;
- edit strategy and city/unit queues;
- show workflow graph progress, blockers, retries, and performance;
- take manual control without stopping the backend.

The browser communicates only with the localhost control API.

### 2. Local control API

Responsibilities:

- authentication token and localhost binding;
- planner status/probe endpoints;
- workflow lifecycle endpoints;
- task approval and cancellation endpoints;
- strategy and plan CRUD;
- event and metrics queries;
- optional server-sent events or WebSocket updates later.

Initial endpoint direction:

```text
GET  /api/state
GET  /api/planner/status
POST /api/planner/probe
POST /api/workflow/tick
POST /api/workflow/start
POST /api/workflow/pause
POST /api/tasks/{id}/approve
POST /api/tasks/{id}/reject
PUT  /api/strategy
PUT  /api/plans/cities/{city_id}
PUT  /api/plans/units/{unit_id}
```

### 3. Workflow orchestrator

The orchestrator owns a sequence of bounded durable ticks, not one function that attempts to finish an entire turn.

A tick may perform several reads but at most one game mutation:

```text
Observe minimum required state
→ Reconcile previous mutation / reload recovery
→ Route one current item
→ Perform exactly one of:
   - gather focused read-only context
   - request one bounded strategic plan
   - create/validate one task
   - execute one approved task
   - attempt end turn
→ Persist the resulting state
→ Return control
```

After any game mutation, the tick ends. A later tick performs fresh-state verification. Continuous operation repeatedly invokes this bounded tick state machine.

Each state and transition must have a persisted status and timing record rather than existing only as control flow inside one Python method.

### 4. Event router

The router maps events to one of four paths:

| Route | Meaning | Example |
|---|---|---|
| deterministic | safe local rule exists | ordinary unit skip, approved city queue |
| planned continuation | existing plan has next step | builder/unit path continuation |
| strategic planner | model decision is justified | settler placement, war contact |
| human only | no automatic action allowed | diplomacy acceptance, irreversible policy choice |

The route decision must be explicit and visible in the frontend.

### 5. Deterministic executors

Executors remain small, typed, and verifiable:

- city production;
- research and civic queues;
- unit movement along approved paths;
- ordinary unit skip/heal/fortify;
- builder movement and improvements;
- later: trade route continuation and safe upgrades.

Every action resolves through the action registry and has required preconditions and postconditions.

### 6. Strategic planner service

The planner receives only event-specific context:

```text
trigger events
+ relevant entities
+ relevant existing plans
+ compact strategy
+ allowed actions and conditions
+ small max_tasks budget
```

It does not receive the full raw snapshot by default and cannot call MCP or mutate the game.

Recommended planner separation later:

- **Strategic planner**: victory path, expansion, city roles, research direction;
- **Tactical exception planner**: one unresolved unit or blocker;
- **Plan reviewer**: checks a proposed plan for risk and contradictions before approval.

These are roles behind one orchestration API, not independent agents competing for game control.

### 7. State, memory, and replay

Persist:

- strategy state;
- city/unit/builder plans;
- workflow node runs;
- tasks and approvals;
- event routing decisions;
- planner requests, responses, request IDs, and timing;
- action calls and verification results;
- replay snapshots.

Long-term memory should contain stable strategic decisions and lessons, not raw every-turn logs.

### 8. Observability

The frontend should distinguish:

- game/FireTuner latency;
- structured state latency;
- MCP action latency;
- planner connection and response-header latency;
- first-byte and completion latency;
- validation or parsing failures;
- task execution and verification latency.

This prevents all failures from being reported as a generic “Agent timeout.”

## Delivery phases

### Phase A — current foundation

- frontend-owned planner connection;
- local secret-holding backend;
- Responses API planner adapter;
- compact event-specific requests;
- deterministic unit blocker resolution;
- explicit approval and single tick;
- planner transport diagnostics.

### Phase B — durable workflow graph

- represent workflow nodes and node runs in SQLite;
- make routing decisions first-class records;
- expose the graph and current node in the frontend;
- support pause/resume at node boundaries;
- add retry policies per node, not one global retry rule.

### Phase C — plan editing workspace

- strategy form;
- city production queues;
- research/civic queues;
- unit and builder plan editor;
- diff view for planner-proposed changes;
- approve/reject/edit before persistence.

### Phase D — separated planning roles

- strategic planner and tactical exception planner;
- optional plan reviewer;
- planner selection by event route;
- per-role model, reasoning effort, timeout, and task budget.

### Phase E — supervised continuous play

- start/pause/stop controls;
- safe continuous tick loop;
- automatic end-turn after verified blocker clearance;
- manual takeover mode;
- long-run replay and regression suite;
- crash/reload recovery validated in real games.

## Safety boundaries

The following remain human-only until dedicated contracts and real-game verification exist:

- declarations of war and peace;
- accepting diplomacy or trade deals;
- city placement;
- city capture decisions;
- purchases with strategic resources or large gold cost;
- World Congress choices;
- major policy/government rebuilds.

## Definition of success

The workflow agent is successful when an ordinary turn can complete with no planner call, a strategic blocker produces one small observable planner request, proposed changes appear in the frontend, and no game mutation occurs outside the action registry and configured approval mode.