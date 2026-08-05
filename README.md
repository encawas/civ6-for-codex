# Civ6 工作流智能体

这是一个面向《文明6：风云变幻》的本地工作流智能体项目。浏览器控制台是监督入口，本地后端负责连接游戏、MCP（模型上下文协议）和规划器。

## Windows 一键启动

首次使用时，在 PowerShell 中克隆仓库并进入项目目录：

```powershell
git clone https://github.com/encawas/civ6-for-codex.git
cd civ6-for-codex
```

然后编辑 `config.toml`，填写可用模型：

```toml
[codex]
backend = "responses"
model = "你的 API 可用模型"
```

并在 Windows 用户环境或当前 PowerShell 中设置 API Key：

```powershell
$env:OPENAI_API_KEY = "你的 OpenAI API Key"
```

### 直接双击启动

双击仓库根目录的：

```text
启动文明6助手.cmd
```

它会调用现有 `start_frontend.ps1`，自动创建/复用 `.venv`、安装依赖、启动本地后端，并在浏览器中打开带随机本地令牌的控制台地址。

### 创建桌面快捷方式

双击一次：

```text
创建桌面快捷方式.cmd
```

脚本会在当前用户桌面创建 **“文明6 工作流助手”** 快捷方式。以后直接双击桌面图标即可启动。

PowerShell 启动方式仍然可用：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_frontend.ps1 -OpenBrowser
```

新版控制台集中显示：

- 游戏连接、回合、Runtime 状态和执行模式；
- 当前工作流阶段与最近 Tick 结果；
- Strategic Proposal、动作确认、Human Wait 恢复和安全重试；
- TurnAction 任务队列与开放阻塞事件；
- 规划器连接、HTTP 诊断和退避状态；
- Tick 查询、规划、动作发送与验证耗时；
- 最近一次持久化 Tick envelope。

控制台仍只监听 `127.0.0.1`。API Key 保留在本地后端中，不会返回给浏览器 JavaScript；页面也不能绕过审批、动作白名单、单 Tick mutation budget、执行锁或后置验证。

## 首次实机测试

保持安全模式：

```toml
[runtime]
execution_mode = "readonly"
auto_end_turn = false
```

启动后先点击 **测试规划器**，确认模型和凭证可用；再运行一次 Tick，检查游戏 ID、回合和事件是否更新。随后再切换到 `confirm` 模式验证任务审批，不要在首次运行直接启用自动写操作。

## 项目结构

```text
civ6-for-codex/
├─ AGENTS.md             # Codex/编码智能体必须遵守的仓库约束
├─ config.toml
├─ start_frontend.ps1
├─ 启动文明6助手.cmd
├─ 创建桌面快捷方式.cmd
├─ pyproject.toml
├─ src/                  # 后端、工作流运行时和控制台页面
├─ tests/                # 自动测试
├─ scripts/              # 安装、快捷方式和辅助脚本
├─ upstream_overlay/     # civ6-mcp 结构化接口补丁
├─ docs/                 # 架构、契约与实机说明
├─ state/                # 本地数据库和规划器运行数据
└─ .github/workflows/    # GitHub 自动测试
```

## 重构约束文档

仓库正在进入架构重构阶段。以下文档是实现约束，优先级高于旧实现细节：

1. `AGENTS.md`：编码智能体入口与不可违反的规则；
2. `docs/REFACTOR_CONSTITUTION.md`：架构总原则和安全不变量；
3. `docs/RUNTIME_STATE_MACHINE.md`：单 Tick 状态机、单次写操作和验证流程；
4. `docs/PLANNER_CALL_POLICY.md`：AI 调用资格、预算、批处理和计划有效期；
5. `docs/DOMAIN_CONTRACTS.md`：状态、事件、决策缺口、计划、任务和动作尝试的数据契约；
6. `docs/REFACTOR_EXECUTION_PLAN.md`：避免大爆炸重写的分阶段实施顺序。

核心目标是：**普通已规划回合零次调用 AI，战略决策回合通常最多一次逻辑调用；规则系统连续执行阶段计划，只有计划失效或出现真正的战略决策缺口才重新规划。**

## Phase 0（阶段零）审计材料

开始修改运行逻辑前，还应阅读：

- `docs/CURRENT_IMPLEMENTATION_AUDIT.md`：当前真实运行时、补丁继承链、持久化与恢复风险；
- `docs/CHARACTERIZATION_TEST_CATALOG.md`：特征测试编号、场景和重构验收矩阵；
- `docs/adr/0001-explicit-runtime-composition.md`：显式组合根决策；
- `docs/adr/0002-stable-decision-identity-and-plan-leases.md`：稳定决策身份与计划租约决策；
- `docs/adr/0003-mutation-attempt-and-verification-boundary.md`：单次写操作、动作尝试和后置验证边界。

## 其他文档

- 前端详细说明：`docs/CONTROL_PANEL.md`
- 原工作流智能体架构说明：`docs/WORKFLOW_AGENT_ARCHITECTURE.md`
- Windows 实机验收：`docs/LIVE_SMOKE_TEST.md`

停止后端：在启动终端按 `Ctrl+C`。
