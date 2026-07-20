from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from pathlib import Path


SUPPORTED_UPSTREAM_COMMIT = "dd2019056371b92ea4854e879ddf05a8cad95e8a"
IMPORT_LINE = "from civ_mcp.workflow_api import mount_workflow_routes\n"
MOUNT_LINE = "    mount_workflow_routes(app)\n\n"
IMPORT_ANCHOR = "from civ_mcp.game_state import GameState\n"
RETURN_ANCHOR = "    return app\n"
UVICORN_CONFIG_PATTERN = re.compile(
    r'(?m)^    uvi_config = uvicorn\.Config\('
    r'web_app, host=(?P<host>"(?:0\.0\.0\.0|127\.0\.0\.1)"), '
    r'port=8000, log_level="info"\)\r?\n'
)
UVICORN_LOG_ISOLATION_MARKER = "access_log=False,\n        log_config=None,"
UVICORN_PATCH = '''    # MCP stdio stdout must contain JSON-RPC only. Keep the upstream bind
    # address, route framework logging through stderr, and suppress access logs.
    uvi_config = uvicorn.Config(
        web_app,
        host={host},
        port=8000,
        log_level="warning",
        access_log=False,
        log_config=None,
    )
'''
LOGGING_ANCHOR = "    logging.basicConfig(level=logging.INFO)\n"
LOGGING_PATCH = '''    # JSON-RPC owns stdout. Force all application/framework logging to stderr
    # even if a dependency configured the root logger before main() runs.
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, force=True)
'''


def _patch_uvicorn_logging(server_text: str) -> str:
    if UVICORN_LOG_ISOLATION_MARKER in server_text:
        return server_text
    match = UVICORN_CONFIG_PATTERN.search(server_text)
    if match is None:
        raise AssertionError("validated Uvicorn configuration anchor disappeared")
    return server_text[: match.start()] + UVICORN_PATCH.format(
        host=match.group("host")
    ) + server_text[match.end() :]


def _patch_root_logging(server_text: str) -> str:
    if LOGGING_PATCH in server_text:
        return server_text
    if LOGGING_ANCHOR not in server_text:
        raise AssertionError("validated logging configuration anchor disappeared")
    return server_text.replace(LOGGING_ANCHOR, LOGGING_PATCH, 1)


def _upstream_head(upstream_root: Path) -> str | None:
    if not (upstream_root / ".git").exists():
        return None
    process = subprocess.run(
        ["git", "-C", str(upstream_root), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        raise SystemExit(
            "unable to determine civ6-mcp commit; review the checkout before applying "
            "the overlay"
        )
    return process.stdout.strip()


def apply_overlay(
    upstream_root: Path,
    *,
    check_only: bool = False,
    allow_unsupported_upstream: bool = False,
) -> None:
    upstream_root = upstream_root.resolve()
    source_root = upstream_root / "src" / "civ_mcp"
    web_api = source_root / "web_api.py"
    server_module = source_root / "server.py"
    target_module = source_root / "workflow_api.py"
    overlay_module = (
        Path(__file__).resolve().parents[1]
        / "upstream_overlay"
        / "src"
        / "civ_mcp"
        / "workflow_api.py"
    )

    if not web_api.exists():
        raise SystemExit(f"not a civ6-mcp checkout: missing {web_api}")
    if not server_module.exists():
        raise SystemExit(f"not a civ6-mcp checkout: missing {server_module}")
    if not overlay_module.exists():
        raise SystemExit(f"overlay source is missing: {overlay_module}")

    head = _upstream_head(upstream_root)
    if (
        head is not None
        and head != SUPPORTED_UPSTREAM_COMMIT
        and not allow_unsupported_upstream
    ):
        raise SystemExit(
            "unsupported civ6-mcp commit: "
            f"expected {SUPPORTED_UPSTREAM_COMMIT}, got {head}. "
            "Review the upstream diff, then rerun with "
            "--allow-unsupported-upstream only if compatibility is confirmed."
        )

    web_text = web_api.read_text(encoding="utf-8")
    if IMPORT_ANCHOR not in web_text:
        raise SystemExit(
            "upstream web_api.py changed: GameState import anchor was not found; "
            "review the upstream diff before applying the overlay"
        )
    if RETURN_ANCHOR not in web_text:
        raise SystemExit(
            "upstream web_api.py changed: return anchor was not found; "
            "review the upstream diff before applying the overlay"
        )

    patched_web = web_text
    if IMPORT_LINE not in patched_web:
        patched_web = patched_web.replace(IMPORT_ANCHOR, IMPORT_ANCHOR + IMPORT_LINE, 1)
    if "mount_workflow_routes(app)" not in patched_web:
        patched_web = patched_web.replace(RETURN_ANCHOR, MOUNT_LINE + RETURN_ANCHOR, 1)

    server_text = server_module.read_text(encoding="utf-8")
    if (
        UVICORN_LOG_ISOLATION_MARKER not in server_text
        and UVICORN_CONFIG_PATTERN.search(server_text) is None
    ):
        raise SystemExit(
            "upstream server.py changed: Uvicorn configuration anchor was not found; "
            "review logging compatibility before applying the overlay"
        )
    if LOGGING_PATCH not in server_text and LOGGING_ANCHOR not in server_text:
        raise SystemExit(
            "upstream server.py changed: logging configuration anchor was not found; "
            "review stdio logging compatibility before applying the overlay"
        )
    patched_server = _patch_uvicorn_logging(server_text)
    patched_server = _patch_root_logging(patched_server)
    if check_only:
        if (
            patched_web != web_text
            or patched_server != server_text
            or not target_module.exists()
        ):
            raise SystemExit("overlay is not installed")
        print(
            "overlay and MCP stdio log isolation are installed"
            + (f" for upstream {head}" if head is not None else "")
        )
        return

    web_backup = web_api.with_suffix(".py.workflow-backup")
    if not web_backup.exists():
        shutil.copy2(web_api, web_backup)
    server_backup = server_module.with_suffix(".py.workflow-backup")
    if not server_backup.exists():
        shutil.copy2(server_module, server_backup)
    shutil.copy2(overlay_module, target_module)
    web_api.write_text(patched_web, encoding="utf-8")
    server_module.write_text(patched_server, encoding="utf-8")
    print(f"installed {target_module}")
    print(f"patched {web_api}")
    print(f"patched {server_module} (stdout JSON-RPC only; logs to stderr)")
    print(f"backup {web_backup}")
    print(f"backup {server_backup}")
    if head is not None:
        print(f"verified upstream commit {head}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Install the structured Civ6 workflow API into a civ6-mcp checkout."
    )
    parser.add_argument("upstream_root", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--allow-unsupported-upstream",
        action="store_true",
        help="Apply after manually reviewing compatibility with a different commit.",
    )
    args = parser.parse_args()
    apply_overlay(
        args.upstream_root,
        check_only=args.check,
        allow_unsupported_upstream=args.allow_unsupported_upstream,
    )


if __name__ == "__main__":
    main()
