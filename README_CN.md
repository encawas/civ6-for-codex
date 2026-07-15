# Civ6 工作流智能体

这是一个独立的《文明6：风云变幻》工作流智能体仓库。前端是操作入口，本地后端负责连接游戏、MCP（模型上下文协议）和规划器。

## 启动前端

```powershell
git clone https://github.com/encawas/civ6-for-codex.git
cd civ6-for-codex
```

编辑根目录的 `config.toml`：

```toml
[codex]
backend = "responses"
model = "你的 API 可用模型"
```

设置 API Key：

```powershell
$env:OPENAI_API_KEY = "你的 OpenAI API Key"
```

启动：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_frontend.ps1
```

脚本会自动：

- 创建 `.venv` Python 虚拟环境；
- 安装项目依赖；
- 创建 `state/` 运行目录；
- 使用根目录的 `config.toml`；
- 启动本地控制后端。

终端会输出类似地址：

```text
http://127.0.0.1:8765/?token=...
```

复制完整地址到浏览器，然后点击 **连接规划器**。

## 首次实机测试

保持以下安全配置：

```toml
[runtime]
execution_mode = "readonly"
auto_end_turn = false
```

## 项目数据

所有运行时数据都保存在仓库根目录的 `state/` 中，并被 `.gitignore` 排除：

```text
state/
├─ civ6-workflow.sqlite3
└─ codex-planner/
```

停止前端：在启动终端按 `Ctrl+C`。
