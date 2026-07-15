# Local workflow control panel

The MVP control panel is a localhost-only supervisory UI. It intentionally does not edit strategy, change execution mode, start a continuous loop, or enable automatic end-turn. Its first purpose is to make the existing workflow observable and reviewable.

The frontend is the control entrypoint. Codex or the Responses API does not launch the page. The browser calls a localhost backend, and that backend holds credentials and connects to the configured planner.

## Configure runtime planning

Runtime planning now uses the Responses API directly rather than starting a full `codex exec` child process. Set a model available to your OpenAI API project in `config.toml`:

```toml
[codex]
backend = "responses"
model = "YOUR_API_MODEL"
reasoning_effort = "low"
```

Provide the API key in the same PowerShell session:

```powershell
$env:OPENAI_API_KEY = "YOUR_API_KEY"
```

A ChatGPT or Codex subscription is not itself an API credential. API usage and billing belong to the API project associated with the key.

The old process path remains available only for diagnostics:

```toml
[codex]
backend = "codex_cli"
```

It is not recommended for per-turn gameplay because a complete CLI initialization can dominate the turn latency.

## Start

```powershell
cd civ6-workflow
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
civ6-control --config config.toml
```

The command starts the localhost backend and prints a URL similar to:

```text
http://127.0.0.1:8765/?token=...
```

It does not open a browser by default. Copy the complete URL into the browser, then use **连接规划器** in the page. The connection probe verifies the configured model and credentials without starting a gameplay planning request. The API key remains in the local backend process and is never returned to browser JavaScript.

Explicit browser opening remains available:

```powershell
civ6-control --config config.toml --open-browser
```

Keep the complete URL. The random token is required by every mutating or state API request. The server listens only on `127.0.0.1`.

Use a different port when needed:

```powershell
civ6-control --config config.toml --port 8877
```

## MVP capabilities

- show the most recently observed game and turn;
- show configured execution mode and automatic end-turn state;
- show whether a workflow tick is running;
- explicitly connect/test the configured planner from the frontend;
- report planner model access without exposing the API key;
- list waiting and historical workflow tasks;
- approve one `awaiting_confirmation` task;
- display open blocker/events;
- display the most recent planner result and request size;
- display planner backend, HTTP status, response-header time, first-byte time, completion time and request ID;
- display the most recent tick timing metrics;
- run one explicitly requested workflow tick.

The control panel does not bypass workflow safety. Approval still uses the SQLite task transition, and a tick still passes through the user-global execution lock, action registry, preconditions and postconditions.

## Reapply the upstream overlay

The overlay now patches both the structured API and Uvicorn logging. Reapply it after updating this branch:

```powershell
python scripts/apply_upstream_overlay.py C:\path\to\civ6-mcp
python scripts/apply_upstream_overlay.py C:\path\to\civ6-mcp --check
```

The installer sets `access_log=False` and `log_config=None` for the embedded Uvicorn server so MCP stdout remains reserved for JSON-RPC. Existing files are backed up before the first patch.

## Safe first use

Keep this configuration for the first run:

```toml
[runtime]
execution_mode = "readonly"
auto_end_turn = false
```

Open the panel, click **连接规划器**, and verify the planner status independently from the game connection. Then run one manual tick and verify that game ID, turn and events update. After that, switch the configuration file to `confirm`, restart the backend, run one tick and verify that proposed tasks appear under **待审批任务** before approving anything.

When `ENDTURN_BLOCKING_UNITS` occurs, the workflow now requests unit rows only for that blocker. Ordinary unplanned units receive a deterministic `unit_skip` task. Settlers, units awaiting promotion, units with combat targets, and malformed existing plans remain blocking strategic decisions.

## Workflow agent direction

The planned end-state is documented in `docs/WORKFLOW_AGENT_ARCHITECTURE.md`. The frontend remains the control plane while deterministic executors, event routing, planning roles, approvals and replay run behind the local orchestration API.

## Deliberately deferred

The MVP does not yet provide:

- continuous start/pause controls;
- execution-mode editing;
- strategy forms;
- city queue drag-and-drop;
- unit map visualization;
- live streaming logs;
- automatic end-turn controls;
- remote access or authentication for other devices.

Those features should be added only after the new Responses API path and deterministic unit blocker logic pass real-game testing.
