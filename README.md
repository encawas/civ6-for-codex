# Civ6 工作流智能体

这是一个面向《文明6：风云变幻》的本地工作流智能体项目。

项目当前暂时位于 `1th/civ6-workflow/`，但该目录已经按独立项目组织。以后可以把整个目录直接拆成单独的 GitHub 仓库。

## 启动前端

在 PowerShell 中进入项目目录：

```powershell
cd civ6-workflow
```

直接运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_frontend.ps1
```

首次运行时，脚本会自动创建 Python 虚拟环境、安装依赖并创建项目内的 `state/` 目录。

终端会输出类似地址：

```text
http://127.0.0.1:8765/?token=...
```

复制完整地址到浏览器，然后点击 **连接规划器**。

## 配置规划器

编辑根目录的 `config.toml`：

```toml
[codex]
backend = "responses"
model = "你的 API 可用模型"
```

在同一个 PowerShell 窗口设置 API Key：

```powershell
$env:OPENAI_API_KEY = "你的 OpenAI API Key"
```

API Key 只由本地后端读取，不会发送到浏览器。

## 首次实机测试

保持安全模式：

```toml
[runtime]
execution_mode = "readonly"
auto_end_turn = false
```

## 项目结构

```text
civ6-workflow/
├─ config.toml
├─ start_frontend.ps1
├─ pyproject.toml
├─ src/                 # 后端与工作流运行时
├─ tests/               # 自动测试
├─ scripts/             # 安装和辅助脚本
├─ upstream_overlay/    # civ6-mcp 结构化接口补丁
├─ docs/                # 架构与实机说明
├─ state/               # 本地数据库和规划器运行数据
└─ .github/workflows/   # 独立仓库 CI
```

## 主要文档

- 前端详细说明：`docs/CONTROL_PANEL.md`
- 工作流智能体架构：`docs/WORKFLOW_AGENT_ARCHITECTURE.md`
- Windows 实机验收：`docs/LIVE_SMOKE_TEST.md`
- 拆成独立仓库：`docs/EXTRACT_STANDALONE_REPO.md`

停止前端：在启动终端按 `Ctrl+C`。
