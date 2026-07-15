import importlib.util
from pathlib import Path

import pytest


def _load_installer():
    script = Path(__file__).resolve().parents[1] / "scripts" / "apply_upstream_overlay.py"
    spec = importlib.util.spec_from_file_location("apply_upstream_overlay", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _fake_checkout(tmp_path: Path) -> Path:
    source = tmp_path / "src" / "civ_mcp"
    source.mkdir(parents=True)
    (source / "web_api.py").write_text(
        "from civ_mcp.game_state import GameState\n\n"
        "def create_app(gs):\n"
        "    app = object()\n"
        "    return app\n",
        encoding="utf-8",
    )
    (source / "server.py").write_text(
        "import uvicorn\n\n"
        "async def lifespan():\n"
        "    web_app = object()\n"
        '    uvi_config = uvicorn.Config(web_app, host="0.0.0.0", port=8000, log_level="info")\n'
        "    return uvi_config\n",
        encoding="utf-8",
    )
    return source


def test_overlay_installer_is_idempotent(tmp_path: Path):
    module = _load_installer()
    source = _fake_checkout(tmp_path)
    web_api = source / "web_api.py"
    server = source / "server.py"

    module.apply_overlay(tmp_path)
    first_web = web_api.read_text(encoding="utf-8")
    first_server = server.read_text(encoding="utf-8")
    module.apply_overlay(tmp_path)
    second_web = web_api.read_text(encoding="utf-8")
    second_server = server.read_text(encoding="utf-8")

    assert first_web == second_web
    assert first_server == second_server
    assert first_web.count("from civ_mcp.workflow_api import mount_workflow_routes") == 1
    assert first_web.count("mount_workflow_routes(app)") == 1
    assert "access_log=False" in first_server
    assert "log_config=None" in first_server
    assert 'log_level="warning"' in first_server
    assert (source / "workflow_api.py").exists()
    assert web_api.with_suffix(".py.workflow-backup").exists()
    assert server.with_suffix(".py.workflow-backup").exists()
    module.apply_overlay(tmp_path, check_only=True)


def test_overlay_rejects_unreviewed_git_commit(tmp_path: Path, monkeypatch):
    module = _load_installer()
    _fake_checkout(tmp_path)
    monkeypatch.setattr(module, "_upstream_head", lambda root: "different-commit")

    with pytest.raises(SystemExit, match="unsupported civ6-mcp commit"):
        module.apply_overlay(tmp_path)

    module.apply_overlay(tmp_path, allow_unsupported_upstream=True)
