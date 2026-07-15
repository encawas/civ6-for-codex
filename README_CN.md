# Civ6 工作流智能体

这是一个自包含的文明6工作流项目。当前暂时放在 `1th/civ6-workflow/`，以后可以把整个目录直接拆成独立 GitHub 仓库。

## 启动前端

在 PowerShell 中进入项目目录：

```powershell
cd civ6-workflow
```

首次启动或普通启动都可以直接运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_frontend.ps1
```

脚本会自动：

- 创建 `.venv` Python 虚拟环境；
- 安装项目依赖；
- 创建 `state/` 运行目录；
- 使用项目内的 `config.toml`；
- 启动本地前端后端。

终端会输出类似地址：

```text
http://127.0.0.1:8765/?token=...
```

复制完整地址到浏览器，然后点击 **连接规划器**。

## 配置规划器

编辑项目根目录的 `config.toml`：

```toml
[codex]
backend = "responses"
model = "你的 API 可用模型"
```

在同一个 PowerShell 窗口设置 API Key：

```powershell
$env:OPENAI_API_KEY = "你的 OpenAI API Key"
```

API Key 只保存在本地后端，不会发送到浏览器。

## 首次实机测试

保持以下安全配置：

```toml
[runtime]
execution_mode = "readonly"
auto_end_turn = false
```

## 项目数据

所有运行时数据都保存在项目目录内：

```text
state/
├─ civ6-workflow.sqlite3
└─ codex-planner/
```

停止前端：在启动终端按 `Ctrl+C`。

## 以后拆成独立仓库

当前项目的所有正式文件都收敛在 `civ6-workflow/`。拆分方法见：

```text
docs/EXTRACT_STANDALONE_REPO.md
```
