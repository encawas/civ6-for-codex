from pathlib import Path

path = Path("tests/test_runtime_loop_closure.py")
text = path.read_text(encoding="utf-8")
old = '''def test_verified_action_does_not_swallow_unrelated_scope_change():
    baseline = _observation(x=1)
    current = normalize_runtime_snapshot(
'''
new = '''def test_verified_action_does_not_swallow_unrelated_scope_change():
    baseline = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            tech_civics={"current_research_type": "TECH_WRITING"},
            units=[
                {
                    "unit_id": 7,
                    "unit_type": "UNIT_SETTLER",
                    "x": 1,
                    "y": 2,
                    "moves_remaining": 1,
                }
            ],
        ),
        observed_at=NOW + timedelta(seconds=1),
    ).canonical
    current = normalize_runtime_snapshot(
'''
if text.count(old) != 1:
    raise SystemExit("unrelated-scope test refinement target is not unique")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
