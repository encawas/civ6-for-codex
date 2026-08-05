@echo off
chcp 65001 >nul
setlocal
set "PROJECT_ROOT=%~dp0"
title 文明6 工作流助手
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PROJECT_ROOT%start_frontend.ps1" -OpenBrowser
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo 启动失败，错误码 %EXIT_CODE%。
  echo 请检查 config.toml、Python 3.12 以及 API Key。
  pause
)
exit /b %EXIT_CODE%
