from pathlib import Path

from civ6_workflow import web_cli, web_ui
from civ6_workflow.control_panel_page import CONTROL_PANEL_HTML


def test_web_cli_installs_one_coherent_control_panel_page():
    assert web_cli.CONTROL_PANEL_HTML == CONTROL_PANEL_HTML
    assert web_ui.CONTROL_PANEL_HTML == CONTROL_PANEL_HTML
    assert 'data-ui-version="2"' in CONTROL_PANEL_HTML
    assert 'id="workflow"' in CONTROL_PANEL_HTML
    assert 'id="humanActions"' in CONTROL_PANEL_HTML
    assert 'id="plannerBtn"' in CONTROL_PANEL_HTML
    assert 'id="tickBtn"' in CONTROL_PANEL_HTML
    assert "/api/state" in CONTROL_PANEL_HTML
    assert "/api/tick" in CONTROL_PANEL_HTML
    assert "/api/planner/probe" in CONTROL_PANEL_HTML
    assert "data-human-action" in CONTROL_PANEL_HTML
    assert "onclick=" not in CONTROL_PANEL_HTML
    assert "<script src=" not in CONTROL_PANEL_HTML
    assert "http://" not in CONTROL_PANEL_HTML
    assert "https://" not in CONTROL_PANEL_HTML


def test_windows_double_click_and_desktop_shortcut_launchers_exist():
    root = Path(__file__).resolve().parents[1]
    launcher = (root / "启动文明6助手.cmd").read_text(encoding="utf-8")
    installer_cmd = (root / "创建桌面快捷方式.cmd").read_text(encoding="utf-8")
    installer = (root / "scripts" / "install_windows_shortcut.ps1").read_text(
        encoding="utf-8"
    )

    assert "start_frontend.ps1" in launcher
    assert "-OpenBrowser" in launcher
    assert "install_windows_shortcut.ps1" in installer_cmd
    assert "CreateShortcut" in installer
    assert "start_frontend.ps1" in installer
    assert "-OpenBrowser" in installer
    assert "127.0.0.1" not in installer
