from pathlib import Path

from civ6_workflow import web_cli, web_ui


def test_web_cli_installs_one_coherent_control_panel_page():
    html = web_cli.CONTROL_PANEL_HTML
    assert web_ui.CONTROL_PANEL_HTML == html
    assert 'data-ui-version="2"' in html
    assert 'id="workflow"' in html
    assert 'id="humanActions"' in html
    assert 'id="plannerBtn"' in html
    assert 'id="tickBtn"' in html
    assert "/api/state" in html
    assert "/api/tick" in html
    assert "/api/planner/probe" in html
    assert "data-human-action" in html
    assert "onclick=" not in html
    assert "<script src=" not in html
    assert "http://" not in html
    assert "https://" not in html
    assert "repeat(3,minmax(0,1fr));row-gap:13px" in html


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
