# Windows Civ6 live smoke test

Run this only on native Windows with Civilization VI, the SDK tuner enabled,
and a disposable Gathering Storm save loaded. Keep `auto_end_turn = false`
throughout the first pass.

## 1. Update and install

```powershell
git switch agent/civ6-workflow-codex-runtime
git pull
cd civ6-workflow
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item config.example.toml config.toml -ErrorAction SilentlyContinue
```

The reviewed upstream commit is:

```text
dd2019056371b92ea4854e879ddf05a8cad95e8a
```

Check and apply the overlay:

```powershell
git -C C:\path\to\civ6-mcp rev-parse HEAD
python scripts/apply_upstream_overlay.py C:\path\to\civ6-mcp
python scripts/apply_upstream_overlay.py C:\path\to\civ6-mcp --check
```

Do not use `--allow-unsupported-upstream` during the first smoke test.

Start the upstream MCP/FastAPI service in its own terminal and leave the game
at the beginning of a turn.

## 2. Offline regression

```powershell
python -m compileall src scripts upstream_overlay
python -m pytest -q tests
```

Do not continue if either command fails.

## 3. Read-only connection check

Set:

```toml
[runtime]
execution_mode = "readonly"
auto_end_turn = false
```

Run:

```powershell
civ6-workflow doctor --config config.toml
civ6-workflow tick --config config.toml
civ6-workflow tick --config config.toml
```

Confirm:

- `civ-mcp` and Codex executables are found;
- configured MCP tools include `set_research`;
- `/api/tech-civics` and `/api/workflow/snapshot` are available;
- no FireTuner connection error occurs;
- `turn` matches the visible game turn;
- `game_id` is identical across both ticks;
- `overview`, `cities` and `tech_civics` contain structured data;
- no city, research, civic or unit action occurs.

## 4. Record a read-only baseline

```powershell
New-Item -ItemType Directory recordings -Force | Out-Null
civ6-workflow record recordings\baseline.json --config config.toml --max-ticks 1
civ6-workflow replay recordings\baseline.json
```

A full replay must finish with:

```json
{
  "remaining_frames": 0,
  "remaining_planner_calls": 0,
  "partial": false
}
```

## 5. Verify confirm-mode semantics before approving anything

Change only:

```toml
[runtime]
execution_mode = "confirm"
auto_end_turn = false
```

Use a state that requires one low-risk action, such as an idle city. Run:

```powershell
civ6-workflow tick --config config.toml
civ6-workflow tasks --config config.toml
```

Before any approval, confirm all newly created tasks show:

```text
awaiting_confirmation
```

Also confirm the game did **not** change. If the first confirm-mode tick performs
an action, stop immediately and keep the save for debugging.

Approve exactly one task:

```powershell
civ6-workflow approve TASK_ID --config config.toml
civ6-workflow tick --config config.toml
civ6-workflow tasks --config config.toml
```

Confirm only the approved action ran and the task became `done` only after its
structured postcondition matched.

## 6. Technology and civic selection

Test only when the game is asking for a new technology or civic.

The strategy state uses exact type names:

```json
{
  "research_queue": ["TECH_MINING", "TECH_POTTERY"],
  "civic_queue": ["CIVIC_CODE_OF_LAWS", "CIVIC_CRAFTSMANSHIP"]
}
```

For each category:

1. run one confirm-mode tick;
2. verify the task is `awaiting_confirmation` and the game is unchanged;
3. approve only that task;
4. run another tick;
5. confirm the upstream tool used `category="tech"` or `category="civic"`;
6. confirm the task becomes `done` only when the structured current type
   matches the planned type.

An unavailable planned type must produce a blocking event rather than silently
selecting another item.

## 7. Builder test

The reviewed upstream unit model does not guarantee a production-origin city
field. Do not rely on `origin_city_id` during this test.

Use either:

- an explicit `expected_unit_id`; or
- one unbound reservation with exactly one newly observed Builder.

Use a visible one-step path to a legal improvement tile and a disposable save.
Confirm in order:

1. the first confirm-mode tick creates an awaiting-approval movement task;
2. no movement occurs before approval;
3. one approval causes at most one movement action;
4. the unit reaches the exact next coordinate before the movement task is done;
5. the improvement task also waits for separate approval;
6. the improvement consumes exactly one charge;
7. visually verify the intended improvement, because map-tile postcondition
   verification is not implemented yet;
8. multiple candidate Builders pause rather than guessing.

## 8. Save rewind

After at least one successful action:

1. load a save from an earlier turn;
2. run one workflow tick;
3. confirm a blocking `turn_rewind_detected` event appears;
4. confirm future-derived plans and executable tasks were cleared;
5. confirm no action and no end turn occurred.

## 9. Concurrent-process safety

With one `civ6-workflow run` active, attempt a second tick from another terminal.
The second process must fail with a message that another process is already
executing a tick. It must not send a game action.

## 10. End-turn gate last

Only after all prior checks and recordings pass, set:

```toml
[runtime]
auto_end_turn = true
```

Use a harmless turn with no diplomacy, trade, policy, production, research,
civic, promotion or unit blockers. Confirm exactly one turn advances. Disable
it immediately if any visible blocker remains unresolved.

## Evidence to keep

- `doctor` output;
- both read-only tick JSON results;
- `recordings/baseline.json`;
- task listings before and after every approval;
- the exact upstream commit;
- MCP error responses and matching visible game state;
- one recording for each successful mutable scenario.
