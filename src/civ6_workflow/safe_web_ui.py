from __future__ import annotations

import asyncio
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

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
        return payload

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
    .replace(_OLD_METRICS_RENDER, _NEW_METRICS_RENDER, 1)
    .replace(_OLD_BINDINGS, _NEW_BINDINGS, 1)
)
