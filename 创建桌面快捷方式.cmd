@echo off
chcp 65001 >nul
setlocal
set "PROJECT_ROOT=%~dp0"
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PROJECT_ROOT%scripts\install_windows_shortcut.ps1"
if errorlevel 1 (
  echo.
  echo 创建快捷方式失败。
)
echo.
pause
