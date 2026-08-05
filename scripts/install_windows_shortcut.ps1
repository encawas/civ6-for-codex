[CmdletBinding()]
param(
    [string]$ShortcutName = "文明6 工作流助手"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$StartScript = Join-Path $ProjectRoot "start_frontend.ps1"
if (-not (Test-Path $StartScript)) {
    throw "找不到启动脚本：$StartScript"
}

$Desktop = [Environment]::GetFolderPath("Desktop")
if (-not $Desktop) {
    throw "无法定位当前用户的桌面目录。"
}

$PowerShell = Join-Path $PSHOME "powershell.exe"
if (-not (Test-Path $PowerShell)) {
    $PowerShell = "powershell.exe"
}

$ShortcutPath = Join-Path $Desktop "$ShortcutName.lnk"
$Shell = New-Object -ComObject WScript.Shell
$Shortcut = $Shell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $PowerShell
$Shortcut.Arguments = "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$StartScript`" -OpenBrowser"
$Shortcut.WorkingDirectory = $ProjectRoot
$Shortcut.Description = "启动文明6本地工作流控制台"
$Shortcut.WindowStyle = 1
$Shortcut.IconLocation = "$env:SystemRoot\System32\SHELL32.dll,13"
$Shortcut.Save()

Write-Host "已创建桌面快捷方式：$ShortcutPath"
