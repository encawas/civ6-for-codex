from pathlib import Path

from civ6_workflow.config import load_config
from civ6_workflow.models import ExecutionMode


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_committed_config_is_safe_and_project_local():
    config_path = PROJECT_ROOT / "config.toml"
    config = load_config(config_path)

    assert config.runtime.execution_mode is ExecutionMode.READONLY
    assert config.runtime.auto_end_turn is False
    assert config.runtime.database_path == "state/civ6-workflow.sqlite3"

    planner = config.codex_config(PROJECT_ROOT)
    assert Path(planner.state_directory) == PROJECT_ROOT / "state" / "codex-planner"


def test_standalone_project_entry_files_exist_at_repository_root():
    required = [
        "README.md",
        "README_CN.md",
        "config.toml",
        "config.example.toml",
        "pyproject.toml",
        "start_frontend.ps1",
        ".gitignore",
        ".github/workflows/tests.yml",
        "src/civ6_workflow/__init__.py",
        "tests/test_engine.py",
    ]
    missing = [path for path in required if not (PROJECT_ROOT / path).exists()]
    assert missing == []
    assert not (PROJECT_ROOT / "civ6-workflow").exists()


def test_readme_describes_current_standalone_repository():
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    assert "1th/civ6-workflow" not in readme
    assert "cd civ6-for-codex" in readme
