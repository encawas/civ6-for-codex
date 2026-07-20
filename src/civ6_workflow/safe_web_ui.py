from __future__ import annotations

import asyncio
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from urllib.parse import unquote, urlparse

from .planner_connection import planner_connection_status, probe_planner_connection
from .web_ui import CONTROL_PANEL_HTML as BASE_CONTROL_PANEL_HTML
from .web_ui import ControlPanelHandler as BaseControlPanelHandler
from .web_ui import ControlPanelState as BaseControlPanelState


class SafeControlPanelState(BaseControlPanelState):
    def snapshot(self):
        payload = super().snapshot()
        diagnostics = self.store.get_meta("last_planner_diagnostics")
        if not isinstance(diagnostics, dict):
            diagnostics = None
        last_probe = self.store.get_meta("last_planner_probe")
        if not isinstance(last_probe, dict):
            last_probe = None
        payload["planner_diagnostics"] = diagnostics
        payload["planner_connection"] = planner_connection_status(self.config.codex)
        payload["last_planner_probe"] = last_probe
        payload["planner_backoff"] = self.store.get_meta("planner_provider_backoff")
        payload["last_event_resolutions"] = self.store.get_meta(
            "last_event_resolutions"
        )
        if payload.get("latest_agent") is not None:
            payload["latest_agent"]["transport"] = diagnostics
        game_id = payload["game"]["game_id"]
        human_actions = {
            "runtime_state": None,
            "human_wait": None,
            "lease_approvals": [],
            "retryable_attempts": [],
        }
        if isinstance(game_id, str) and game_id:
            runtime_state = self.store.load_runtime_state(game_id)
            human_actions["runtime_state"] = runtime_state.value
            human_actions["human_wait"] = self.store.human_wait_context(game_id)
            human_actions["lease_approvals"] = [
                {
                    "plan_lease_id": lease.plan_lease_id,
                    "plan_id": lease.plan_id,
                    "scope": lease.scope,
                    "decision_gap_ids": list(lease.decision_gap_ids),
                    "task_ids": list(lease.task_ids),
                }
                for lease in self.store.list_plan_leases(game_id)
                if lease.status.value == "AWAITING_APPROVAL"
            ]
            human_actions["retryable_attempts"] = self.store.retryable_failed_attempts(
                game_id
            )
        payload["human_actions"] = human_actions
        return payload

    def confirm_task(self, game_id: str, task_id: str) -> tuple[bool, str]:
        if self.store.approve_task(game_id, task_id, "control-panel-user"):
            return True, "confirmation recorded; task is eligible for a later tick"
        return False, "task is not awaiting confirmation for this game"

    def reject_task(self, game_id: str, task_id: str) -> tuple[bool, str]:
        if self.store.reject_task_confirmation(game_id, task_id):
            return True, "confirmation rejected; the task was cancelled"
        return False, "task is not awaiting confirmation for this game"

    def decide_lease(
        self, game_id: str, plan_lease_id: str, *, approved: bool
    ) -> tuple[bool, str]:
        return self.store.record_lease_approval(
            game_id,
            plan_lease_id,
            approved=approved,
        )

    def retry_attempt(
        self, game_id: str, action_attempt_id: str
    ) -> tuple[bool, str]:
        return self.store.retry_failed_attempt_if_safe(game_id, action_attempt_id)

    def request_resume(self, game_id: str) -> tuple[bool, str]:
        if self.store.request_human_resume(game_id):
            return True, "resume recorded; the next tick will re-evaluate the wait"
        return False, "workflow is not awaiting human review for this game"

    def planner_status(self):
        return {
            "status": planner_connection_status(self.config.codex),
            "last_probe": self.store.get_meta("last_planner_probe"),
        }

    def probe_planner(self):
        result = asyncio.run(probe_planner_connection(self.config.codex))
        self.store.set_meta("last_planner_probe", result)
        return result


class SafeControlPanelHandler(BaseControlPanelHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/planner/status":
            if not self._authorized(parsed):
                return
            self._send_json(self.server.control.planner_status())
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        if len(parts) == 5 and parts[:2] == ["api", "games"] and parts[3:] == [
            "workflow",
            "resume",
        ]:
            if not self._authorized(parsed):
                return
            ok, reason = self.server.control.request_resume(parts[2])
            self._send_human_action(ok, reason, game_id=parts[2])
            return
        if len(parts) == 6 and parts[:2] == ["api", "games"] and parts[3] == "tasks":
            if not self._authorized(parsed):
                return
            game_id, task_id, decision = parts[2], parts[4], parts[5]
            if decision == "confirm":
                ok, reason = self.server.control.confirm_task(game_id, task_id)
            elif decision == "reject":
                ok, reason = self.server.control.reject_task(game_id, task_id)
            else:
                self._send_json({"error": "unknown task decision"}, HTTPStatus.NOT_FOUND)
                return
            self._send_human_action(ok, reason, game_id=game_id, task_id=task_id)
            return
        if len(parts) == 6 and parts[:2] == ["api", "games"] and parts[3] == "leases":
            if not self._authorized(parsed):
                return
            game_id, lease_id, decision = parts[2], parts[4], parts[5]
            if decision not in {"approve", "reject"}:
                self._send_json({"error": "unknown lease decision"}, HTTPStatus.NOT_FOUND)
                return
            ok, reason = self.server.control.decide_lease(
                game_id,
                lease_id,
                approved=decision == "approve",
            )
            self._send_human_action(ok, reason, game_id=game_id, plan_lease_id=lease_id)
            return
        if len(parts) == 6 and parts[:2] == ["api", "games"] and parts[3] == "attempts":
            if not self._authorized(parsed):
                return
            game_id, attempt_id, decision = parts[2], parts[4], parts[5]
            if decision != "retry":
                self._send_json({"error": "unknown attempt decision"}, HTTPStatus.NOT_FOUND)
                return
            ok, reason = self.server.control.retry_attempt(game_id, attempt_id)
            self._send_human_action(
                ok,
                reason,
                game_id=game_id,
                action_attempt_id=attempt_id,
            )
            return
        if parsed.path == "/api/workflow/resume":
            if not self._authorized(parsed):
                return
            game_id = self.server.control.store.get_meta("last_game_id")
            if not isinstance(game_id, str):
                self._send_json({"error": "no observed game"}, HTTPStatus.CONFLICT)
                return
            ok, reason = self.server.control.request_resume(game_id)
            if not ok:
                self._send_json({"error": reason}, HTTPStatus.CONFLICT)
                return
            self._send_json({"ok": True, "resume_requested": True})
            return
        if parsed.path == "/api/planner/probe":
            if not self._authorized(parsed):
                return
            result = self.server.control.probe_planner()
            ok = bool(result.get("ok"))
            payload = {"ok": ok, "result": result}
            if not ok:
                payload["error"] = str(result.get("error") or "planner probe failed")
            self._send_json(
                payload,
                HTTPStatus.OK if ok else HTTPStatus.BAD_GATEWAY,
            )
            return
        super().do_POST()

    def _send_human_action(
        self,
        ok: bool,
        reason: str,
        **identifiers: str,
    ) -> None:
        payload = {"ok": ok, "reason": reason, **identifiers}
        self._send_json(payload, HTTPStatus.OK if ok else HTTPStatus.CONFLICT)


class SafeControlPanelHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], control: SafeControlPanelState):
        super().__init__(address, SafeControlPanelHandler)
        self.control = control


_OLD_HEADER = '<div class="actions"><button class="btn" id="refreshBtn">刷新</button><button class="btn primary" id="tickBtn">运行一次 Tick</button></div>'
_NEW_HEADER = '<div class="actions"><button class="btn" id="plannerBtn">连接规划器</button><button class="btn" id="refreshBtn">刷新</button><button class="btn primary" id="tickBtn">运行一次 Tick</button></div>'

_OLD_AGENT_RENDER = """const a=d.latest_agent;$('agent').innerHTML=a?`<div class="metrics">${metric('回合',a.turn)}${metric('结果',a.success?'成功':'失败')}${metric('耗时',a.duration_seconds,'s')}${metric('请求体',Math.round(a.request_bytes/1024*10)/10,' KB')}</div>${a.error?`<div class="error" style="margin-top:10px">${esc(a.error)}</div>`:''}`:'<div class="empty">尚无规划调用</div>';"""
_NEW_AGENT_RENDER = """const a=d.latest_agent;const p=d.planner_diagnostics;const c=d.planner_connection;const probe=d.last_planner_probe;const backoff=d.planner_backoff;const baseAgent=a?`<div class="metrics">${metric('回合',a.turn)}${metric('结果',a.success?'成功':'失败')}${metric('耗时',a.duration_seconds,'s')}${metric('请求体',Math.round(a.request_bytes/1024*10)/10,' KB')}</div>${a.error?`<div class="error" style="margin-top:10px">${esc(a.error)}</div>`:''}`:'<div class="empty">尚无规划调用</div>';const connection=c?`<div class="metrics" style="margin-top:12px">${metric('连接方式','前端 → 本地后端 → '+c.backend)}${metric('模型',c.model||'未配置')}${metric('凭证',c.credential_present===false?'缺失':(c.configured?'已配置':'待配置'))}${metric('密钥进浏览器',c.secret_exposed_to_browser?'是':'否')}</div>`:'';const probeBlock=probe?`<div class="metrics" style="margin-top:12px">${metric('连接测试',probe.ok?'成功':'失败')}${metric('测试耗时',probe.duration_seconds,'s')}${metric('HTTP',probe.http_status)}${metric('请求 ID',probe.request_id||'—')}</div>${probe.error?`<div class="error" style="margin-top:10px">${esc(probe.error)}</div>`:''}`:'';const transport=p?`<div class="metrics" style="margin-top:12px">${metric('运行后端',p.backend)}${metric('HTTP',p.http_status)}${metric('HTTP 重试',p.attempt_count)}${metric('响应头',p.connect_and_headers_seconds,'s')}${metric('首字节',p.first_byte_seconds,'s')}${metric('完成',p.completion_seconds,'s')}${metric('响应体',Math.round((p.response_bytes||0)/1024*10)/10,' KB')}</div>${p.request_id?`<pre style="margin-top:10px">request_id: ${esc(p.request_id)}</pre>`:''}`:'';const wait=backoff?.until_epoch?`<div class="error" style="margin-top:10px">规划器退避：${esc(backoff.category||'transient_provider_failure')}，${esc(backoff.delay_seconds||'—')} 秒</div>`:'';$('agent').innerHTML=connection+probeBlock+baseAgent+transport+wait;$('plannerBtn').textContent=probe?.ok?'规划器已连接':'连接规划器';"""

_OLD_METRICS_RENDER = """const m=d.latest_metrics;$('metrics').innerHTML=m?`<div class="metrics">${metric('总耗时',m.total_seconds,'s')}${metric('状态读取',m.state_query_seconds,'s')}${metric('任务执行',m.task_execution_seconds,'s')}${metric('Agent',m.agent_seconds,'s')}${metric('I/O 调用',m.mcp_call_count)}${metric('Agent 调用',m.agent_call_count)}</div>`:'<div class="empty">尚无 Tick 指标</div>';"""
_NEW_METRICS_RENDER = """const m=d.latest_metrics;$('metrics').innerHTML=m?`<div class="metrics">${metric('总耗时',m.total_seconds,'s')}${metric('状态读取',m.state_query_seconds,'s')}${metric('任务执行',m.task_execution_seconds,'s')}${metric('规划耗时',m.agent_seconds,'s')}${metric('I/O 调用',m.mcp_call_count)}${metric('规划尝试',m.agent_attempt_count??m.agent_call_count)}${metric('规划成功',m.agent_success_count??0)}${metric('只读查询',m.information_query_count??0)}</div>`:'<div class="empty">尚无 Tick 指标</div>';"""

_OLD_BINDINGS = """$('refreshBtn').onclick=refresh;$('tickBtn').onclick=tick;refresh();setInterval(refresh,2000);"""
_NEW_BINDINGS = """async function connectPlanner(){const b=$('plannerBtn');b.disabled=true;b.textContent='连接中…';try{await api('/api/planner/probe',{method:'POST',body:'{}'})}catch(e){alert(e.message)}finally{b.disabled=false;await refresh()}}$('plannerBtn').onclick=connectPlanner;$('refreshBtn').onclick=refresh;$('tickBtn').onclick=tick;refresh();setInterval(refresh,2000);"""

_HUMAN_ACTIONS_MARKUP = """
<section class="grid" style="margin-top:16px"><article class="card"><div class="cardhead"><h2>Manual Actions</h2><span class="count" id="humanActionCount">0</span></div><div class="body" id="humanActions"></div></article></section>
"""

_HUMAN_ACTIONS_SCRIPT = """
let humanActionStatus='';
function humanButton(label,action,game,target){return `<button class="btn" data-human-action="${esc(action)}" data-game-id="${esc(game)}" data-target-id="${esc(target)}">${esc(label)}</button>`}
function humanActionRow(title,detail,buttons){return `<div class="task"><div><div class="task-title">${esc(title)}</div><div class="reason">${esc(detail)}</div></div><div class="actions">${buttons}</div></div>`}
function renderHumanActions(d){const h=d.human_actions||{};const game=d.game?.game_id||'';const rows=[];if(h.runtime_state==='AWAITING_HUMAN'){rows.push(humanActionRow('Workflow requires review',h.human_wait?.blocking_reason||'A fresh observation will be evaluated after resume.',humanButton('Resume','resume',game,game)))}for(const task of d.waiting_tasks||[]){rows.push(humanActionRow(`Task: ${task.action_type}`,task.reason||task.task_id,humanButton('Confirm','confirm-task',game,task.task_id)+humanButton('Reject','reject-task',game,task.task_id)))}for(const lease of h.lease_approvals||[]){rows.push(humanActionRow(`Plan approval: ${lease.scope}`,lease.plan_id,humanButton('Approve','approve-lease',game,lease.plan_lease_id)+humanButton('Reject','reject-lease',game,lease.plan_lease_id)))}for(const attempt of h.retryable_attempts||[]){rows.push(humanActionRow(`Retry: ${attempt.action_type}`,attempt.reason,humanButton('Retry','retry-attempt',game,attempt.action_attempt_id)))}$('humanActionCount').textContent=rows.length;$('humanActions').innerHTML=(humanActionStatus?`<div class="reason" style="margin-bottom:10px">${esc(humanActionStatus)}</div>`:'')+(rows.length?rows.join(''):'<div class="empty">No manual action is currently available.</div>')}
async function performHumanAction(button){const action=button.dataset.humanAction;const game=button.dataset.gameId;const target=button.dataset.targetId;const path={resume:`/api/games/${encodeURIComponent(game)}/workflow/resume`,'confirm-task':`/api/games/${encodeURIComponent(game)}/tasks/${encodeURIComponent(target)}/confirm`,'reject-task':`/api/games/${encodeURIComponent(game)}/tasks/${encodeURIComponent(target)}/reject`,'approve-lease':`/api/games/${encodeURIComponent(game)}/leases/${encodeURIComponent(target)}/approve`,'reject-lease':`/api/games/${encodeURIComponent(game)}/leases/${encodeURIComponent(target)}/reject`,'retry-attempt':`/api/games/${encodeURIComponent(game)}/attempts/${encodeURIComponent(target)}/retry`}[action];button.disabled=true;try{const result=await api(path,{method:'POST',body:'{}'});humanActionStatus=result.reason||'Action recorded.';if(action==='resume'){const tickResult=await api('/api/tick',{method:'POST',body:'{}'});humanActionStatus=`${humanActionStatus} ${tickResult.result?.outcome||'Re-evaluation completed.'}`}}catch(error){humanActionStatus=error.message}finally{await refresh()}}
document.addEventListener('click',event=>{const button=event.target.closest('[data-human-action]');if(button){performHumanAction(button)}});
"""

for anchor, name in (
    (_OLD_HEADER, "control panel header"),
    (_OLD_AGENT_RENDER, "control panel planner render"),
    (_OLD_METRICS_RENDER, "control panel metrics render"),
    (_OLD_BINDINGS, "control panel bindings"),
):
    if anchor not in BASE_CONTROL_PANEL_HTML:
        raise RuntimeError(f"{name} anchor changed")

ENHANCED_CONTROL_PANEL_HTML = (
    BASE_CONTROL_PANEL_HTML.replace(_OLD_HEADER, _NEW_HEADER, 1)
    .replace(_OLD_AGENT_RENDER, _NEW_AGENT_RENDER, 1)
    .replace(_OLD_METRICS_RENDER, _NEW_METRICS_RENDER + "renderHumanActions(d);", 1)
    .replace(_OLD_BINDINGS, _HUMAN_ACTIONS_SCRIPT + _NEW_BINDINGS, 1)
    .replace("</main>", _HUMAN_ACTIONS_MARKUP + "</main>", 1)
)
