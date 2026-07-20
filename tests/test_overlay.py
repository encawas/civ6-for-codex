import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


def _load_installer():
    script = Path(__file__).resolve().parents[1] / "scripts" / "apply_upstream_overlay.py"
    spec = importlib.util.spec_from_file_location("apply_upstream_overlay", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _fake_checkout(tmp_path: Path, *, host: str = "127.0.0.1") -> Path:
    source = tmp_path / "src" / "civ_mcp"
    source.mkdir(parents=True)
    (source / "uvicorn.py").write_text("# test stub\n", encoding="utf-8")
    (source / "web_api.py").write_text(
        "from civ_mcp.game_state import GameState\n\n"
        "def create_app(gs):\n"
        "    app = object()\n"
        "    return app\n",
        encoding="utf-8",
    )
    (source / "server.py").write_text(
        "import json\n"
        "import logging\n"
        "import sys\n"
        "import uvicorn\n\n"
        "async def lifespan():\n"
        "    web_app = object()\n"
        f'    uvi_config = uvicorn.Config(web_app, host="{host}", port=8000, log_level="info")\n'
        "    return uvi_config\n\n"
        "def main():\n"
        "    logging.basicConfig(level=logging.INFO)\n"
        "    for tick in range(1, 4):\n"
        "        logging.getLogger(__name__).info('tick %s', tick)\n"
        "        sys.stdout.write(json.dumps({'jsonrpc': '2.0', 'id': tick, 'result': {}}) + '\\n')\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    main()\n",
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


def test_overlay_keeps_stdout_json_rpc_only(tmp_path: Path):
    module = _load_installer()
    source = _fake_checkout(tmp_path, host="0.0.0.0")
    module.apply_overlay(tmp_path)

    process = subprocess.run(
        [sys.executable, str(source / "server.py")],
        check=True,
        capture_output=True,
        text=True,
    )
    stdout_lines = [line for line in process.stdout.splitlines() if line]
    assert [json.loads(line) for line in stdout_lines] == [
        {"jsonrpc": "2.0", "id": 1, "result": {}},
        {"jsonrpc": "2.0", "id": 2, "result": {}},
        {"jsonrpc": "2.0", "id": 3, "result": {}},
    ]
    assert "tick 1" in process.stderr
    assert "tick 2" in process.stderr
    assert "tick 3" in process.stderr
    assert "tick 1" not in process.stdout
    assert "tick 2" not in process.stdout
    assert "tick 3" not in process.stdout
