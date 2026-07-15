[CmdletBinding()]
param(
    [int]$Port = 8765,
    [switch]$OpenBrowser,
    [switch]$Reinstall
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $ProjectRoot

try {
    if (-not (Test-Path "config.toml")) {
        if (-not (Test-Path "config.example.toml")) {
            throw "缺少 config.toml 和 config.example.toml。"
        }
        Copy-Item "config.example.toml" "config.toml"
        Write-Host "已从 config.example.toml 创建 config.toml。"
    }

    New-Item -ItemType Directory -Force -Path "state" | Out-Null

    $Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
    $Control = Join-Path $ProjectRoot ".venv\Scripts\civ6-control.exe"

    if (-not (Test-Path $Python)) {
        if (Get-Command py -ErrorAction SilentlyContinue) {
            & py -3.12 -m venv .venv
        }
        elseif (Get-Command python -ErrorAction SilentlyContinue) {
            & python -m venv .venv
        }
        else {
            throw "未找到 Python。项目需要 Python 3.12 或更高版本。"
        }
    }

    if ($Reinstall -or -not (Test-Path $Control)) {
        & $Python -m pip install -e ".[dev]"
        if ($LASTEXITCODE -ne 0) {
            throw "Python 依赖安装失败。"
        }
    }

    $Arguments = @(
        "--config", (Join-Path $ProjectRoot "config.toml"),
        "--port", $Port
    )
    if ($OpenBrowser) {
        $Arguments += "--open-browser"
    }

    & $Control @Arguments
}
finally {
    Pop-Location
}
