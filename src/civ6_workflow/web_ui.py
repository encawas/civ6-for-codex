from __future__ import annotations

import json
import secrets
import sys
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

from .config import AppConfig
from .models import TaskStatus
from .store import WorkflowStore


TickRunner = Callable[[], Any]


@dataclass(slots=True)
class ControlPanelState:
    """Thread-safe state and read models for the local control panel."""

    config: AppConfig
    store: WorkflowStore
    run_tick_callback: TickRunner
    token: str = field(default_factory=lambda: secrets.token_urlsafe(24))
    started_at: float = field(default_factory=time.time)
    _tick_lock: threading.Lock = field(default_factory=threading.Lock)
    _state_lock: threading.Lock = field(default_factory=threading.Lock)
    _last_tick: dict[str, Any] | None = None
    _last_error: str | None = None

    @property
    def tick_running(self) -> bool:
        return self._tick_lock.locked()

    def snapshot(self) -> dict[str, Any]:
        game_id = self.store.get_meta("last_game_id")
        turn = self.store.get_meta("last_observed_turn")
        tasks = self.store.list_tasks(game_id) if isinstance(game_id, str) else []
        task_rows = [task.model_dump(mode="json") for task in tasks]
        task_counts = {status.value: 0 for status in TaskStatus}
        for task in tasks:
            task_counts[task.status.value] = task_counts.get(task.status.value, 0) + 1

        open_events: list[dict[str, Any]] = []
        latest_agent: dict[str, Any] | None = None
        latest_metrics: dict[str, Any] | None = None
        if isinstance(game_id, str):
            with self.store._connect() as conn:  # package-internal dashboard query
                rows = conn.execute(
                    """
                    SELECT event_json, seen_count, status
                    FROM event_log
                    WHERE game_id=? AND status='open'
                    ORDER BY level DESC, last_seen_turn DESC, dedupe_key
                    LIMIT 20
                    """,
                    (game_id,),
                ).fetchall()
                for row in rows:
                    event = self.store._load(row["event_json"])
                    event["seen_count"] = int(row["seen_count"])
                    event["status"] = row["status"]
                    open_events.append(event)

                agent_row = conn.execute(
                    """
                    SELECT turn, request_id, request_json, success, error,
                           duration_seconds, created_at
                    FROM agent_runs
                    WHERE game_id=?
                    ORDER BY run_id DESC
                    LIMIT 1
                    """,
                    (game_id,),
                ).fetchone()
                if agent_row is not None:
                    request_json = str(agent_row["request_json"] or "")
                    latest_agent = {
                        "turn": int(agent_row["turn"]),
                        "request_id": agent_row["request_id"],
                        "success": bool(agent_row["success"]),
                        "error": agent_row["error"],
                        "duration_seconds": float(agent_row["duration_seconds"]),
                        "request_bytes": len(request_json.encode("utf-8")),
                        "created_at": agent_row["created_at"],
                    }

                metrics_row = conn.execute(
                    """
                    SELECT turn, metrics_json, created_at
                    FROM turn_metrics
                    WHERE game_id=?
                    ORDER BY turn DESC
                    LIMIT 1
                    """,
                    (game_id,),
                ).fetchone()
                if metrics_row is not None:
                    latest_metrics = self.store._load(metrics_row["metrics_json"])
                    latest_metrics["turn"] = int(metrics_row["turn"])
                    latest_metrics["created_at"] = metrics_row["created_at"]

        with self._state_lock:
            last_tick = None if self._last_tick is None else dict(self._last_tick)
            last_error = self._last_error

        waiting = [
            task
            for task in task_rows
            if task["status"] == TaskStatus.AWAITING_CONFIRMATION.value
        ]
        return {
            "server": {
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "tick_running": self.tick_running,
                "last_error": last_error,
            },
            "config": {
                "execution_mode": self.config.runtime.execution_mode.value,
                "auto_end_turn": self.config.runtime.auto_end_turn,
                "database_path": str(self.store.path),
                "state_api": self.config.state_api.base_url,
            },
            "game": {
                "game_id": game_id,
                "turn": turn,
                "observed": isinstance(game_id, str) and bool(game_id),
            },
            "task_counts": task_counts,
            "tasks": task_rows,
            "waiting_tasks": waiting,
            "open_events": open_events,
            "latest_agent": latest_agent,
            "latest_metrics": latest_metrics,
            "last_tick": last_tick,
        }

    def approve(self, task_id: str) -> bool:
        game_id = self.store.get_meta("last_game_id")
        if not isinstance(game_id, str) or not game_id:
            return False
        return self.store.approve_task(game_id, task_id)

    def run_tick(self) -> dict[str, Any]:
        if not self._tick_lock.acquire(blocking=False):
            raise RuntimeError("a workflow tick is already running")
        try:
            result = self.run_tick_callback()
            payload = (
                result.model_dump(mode="json")
                if hasattr(result, "model_dump")
                else dict(result)
            )
            with self._state_lock:
                self._last_tick = payload
                self._last_error = None
            return payload
        except Exception as exc:
            with self._state_lock:
                self._last_error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            self._tick_lock.release()


class ControlPanelHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], control: ControlPanelState):
        super().__init__(address, ControlPanelHandler)
        self.control = control


class ControlPanelHandler(BaseHTTPRequestHandler):
    server: ControlPanelHTTPServer

    def log_message(self, fmt: str, *args: object) -> None:
        # Keep control-panel diagnostics out of stdout. This matters when the
        # workflow is launched near JSON-RPC stdio processes.
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), fmt % args)
        )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(CONTROL_PANEL_HTML)
            return
        if parsed.path == "/health":
            self._send_json({"ok": True})
            return
        if parsed.path == "/api/state":
            if not self._authorized(parsed):
                return
            self._send_json(self.server.control.snapshot())
            return
        self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if not self._authorized(parsed):
            return
        if parsed.path == "/api/tick":
            try:
                result = self.server.control.run_tick()
            except RuntimeError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                return
            except Exception as exc:
                self._send_json(
                    {"error": f"{type(exc).__name__}: {exc}"},
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                return
            self._send_json({"ok": True, "result": result})
            return

        prefix = "/api/tasks/"
        suffix = "/approve"
        if parsed.path.startswith(prefix) and parsed.path.endswith(suffix):
            task_id = unquote(parsed.path[len(prefix) : -len(suffix)]).strip("/")
            if not task_id:
                self._send_json({"error": "task id is required"}, HTTPStatus.BAD_REQUEST)
                return
            if not self.server.control.approve(task_id):
                self._send_json(
                    {"error": "task is not awaiting confirmation"},
                    HTTPStatus.CONFLICT,
                )
                return
            self._send_json({"ok": True, "task_id": task_id})
            return

        self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def _authorized(self, parsed) -> bool:
        header_token = self.headers.get("X-Civ6-Token")
        query_token = parse_qs(parsed.query).get("token", [None])[0]
        supplied = header_token or query_token
        if not secrets.compare_digest(supplied or "", self.server.control.token):
            self._send_json({"error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
            return False
        return True

    def _send_json(
        self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK
    ) -> None:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, html: str) -> None:
        raw = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
            "connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'",
        )
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


CONTROL_PANEL_HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Civ VI Workflow Control</title>
<style>
:root{color-scheme:dark;--bg:#071018;--panel:#0d1822;--panel2:#111f2b;--line:#243442;--text:#edf2f5;--muted:#91a1ad;--gold:#d5ae59;--gold2:#f1ce79;--green:#55c793;--red:#f06c75;--blue:#62a9e8;--orange:#e7a757}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 70% -20%,#17334a 0,transparent 38%),var(--bg);font:14px/1.45 Inter,Segoe UI,Microsoft YaHei,sans-serif;color:var(--text)}
button{font:inherit}.shell{max-width:1440px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:20px;align-items:flex-start;margin-bottom:20px}.brand small{display:block;letter-spacing:.22em;color:var(--gold);font-weight:700}.brand h1{margin:5px 0 4px;font-family:Georgia,serif;font-size:30px;font-weight:500}.brand p{margin:0;color:var(--muted)}
.actions{display:flex;gap:10px;align-items:center}.btn{border:1px solid var(--line);border-radius:8px;padding:9px 14px;background:#142431;color:var(--text);cursor:pointer;transition:.15s}.btn:hover{border-color:#476073}.btn.primary{background:linear-gradient(180deg,#d7b45f,#af8738);border-color:#e7c87e;color:#17140d;font-weight:750}.btn:disabled{opacity:.45;cursor:not-allowed}
.statusbar{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-bottom:18px}.stat{background:rgba(13,24,34,.86);border:1px solid var(--line);border-radius:10px;padding:13px 15px}.stat label{display:block;color:var(--muted);font-size:12px;margin-bottom:4px}.stat strong{font-size:16px}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);margin-right:7px}.dot.ok{background:var(--green);box-shadow:0 0 12px #55c79388}
.grid{display:grid;grid-template-columns:1.25fr .75fr;gap:16px}.stack{display:grid;gap:16px}.card{background:rgba(13,24,34,.92);border:1px solid var(--line);border-radius:12px;overflow:hidden}.cardhead{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;border-bottom:1px solid var(--line);background:rgba(17,31,43,.7)}.cardhead h2{font-size:14px;margin:0;letter-spacing:.04em}.count{font-size:12px;color:var(--muted)}.body{padding:14px 16px}.empty{padding:24px 12px;text-align:center;color:var(--muted)}
.task{display:grid;grid-template-columns:1fr auto;gap:12px;padding:12px 0;border-bottom:1px solid #1c2b37}.task:last-child{border:0}.task-title{font-weight:700}.meta{color:var(--muted);font-size:12px;margin-top:3px}.reason{margin-top:7px;color:#cbd5dc}.pill{display:inline-flex;align-items:center;border:1px solid #354957;border-radius:999px;padding:2px 8px;font-size:11px;color:#b8c5ce}.pill.awaiting_confirmation{color:var(--gold2);border-color:#765f2c}.pill.ready,.pill.done{color:var(--green);border-color:#2e694f}.pill.failed,.pill.escalated,.pill.blocked{color:#ff8b92;border-color:#733b42}
.event{padding:11px 0;border-bottom:1px solid #1c2b37}.event:last-child{border:0}.eventtop{display:flex;justify-content:space-between;gap:12px}.eventtype{font-weight:700}.level{font-size:11px;border-radius:5px;padding:2px 6px;background:#243340}.level.l3{background:#6a3035;color:#ffd5d8}.event p{color:#b6c3cc;margin:6px 0 0;white-space:pre-wrap}
.metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.metric{padding:11px;background:#101e29;border:1px solid #20313e;border-radius:8px}.metric label{display:block;color:var(--muted);font-size:11px}.metric b{display:block;margin-top:3px}.error{border-left:3px solid var(--red);background:#2a161a;padding:10px 12px;color:#ffb8bd;border-radius:4px;word-break:break-word}.oktext{color:var(--green)}.badtext{color:var(--red)}
pre{margin:0;white-space:pre-wrap;word-break:break-word;color:#b9c6ce;font:12px/1.55 ui-monospace,SFMono-Regular,Consolas,monospace}.footer{text-align:center;color:#61717c;font-size:11px;margin-top:20px}
@media(max-width:900px){.top{display:block}.actions{margin-top:14px}.statusbar{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}}
</style>
</head>
<body><main class="shell">
<header class="top"><div class="brand"><small>CIVILIZATION VI</small><h1>Workflow Control</h1><p>本地监督、审批与单步执行面板</p></div><div class="actions"><button class="btn" id="refreshBtn">刷新</button><button class="btn primary" id="tickBtn">运行一次 Tick</button></div></header>
<section class="statusbar">
<div class="stat"><label>游戏</label><strong id="gameStatus"><span class="dot"></span>未观测</strong></div>
<div class="stat"><label>当前回合</label><strong id="turn">—</strong></div>
<div class="stat"><label>执行模式</label><strong id="mode">—</strong></div>
<div class="stat"><label>自动结束回合</label><strong id="autoEnd">—</strong></div>
<div class="stat"><label>工作流</label><strong id="runtime">空闲</strong></div>
</section>
<div id="errorBox"></div>
<section class="grid">
<div class="stack">
<article class="card"><div class="cardhead"><h2>待审批任务</h2><span class="count" id="waitingCount">0</span></div><div class="body" id="waitingTasks"></div></article>
<article class="card"><div class="cardhead"><h2>全部任务</h2><span class="count" id="taskCount">0</span></div><div class="body" id="allTasks"></div></article>
</div>
<div class="stack">
<article class="card"><div class="cardhead"><h2>当前阻塞与事件</h2><span class="count" id="eventCount">0</span></div><div class="body" id="events"></div></article>
<article class="card"><div class="cardhead"><h2>最近规划调用</h2></div><div class="body" id="agent"></div></article>
<article class="card"><div class="cardhead"><h2>最近 Tick 指标</h2></div><div class="body" id="metrics"></div></article>
</div>
</section><div class="footer">仅监听本机 · 所有动作仍受工作流审批和执行锁约束</div>
</main>
<script>
const token=new URLSearchParams(location.search).get('token')||'';
const $=id=>document.getElementById(id);
const esc=v=>String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
async function api(path,opts={}){const r=await fetch(path,{...opts,headers:{'X-Civ6-Token':token,'Content-Type':'application/json',...(opts.headers||{})}});const d=await r.json().catch(()=>({error:`HTTP ${r.status}`}));if(!r.ok)throw new Error(d.error||`HTTP ${r.status}`);return d}
function taskHtml(t,approve=false){return `<div class="task"><div><div class="task-title">${esc(t.action_type)} · ${esc(t.entity_type)}:${esc(t.entity_id)}</div><div class="meta"><span class="pill ${esc(t.status)}">${esc(t.status)}</span>　回合 ${esc(t.due_turn)}　${esc(t.task_id)}</div><div class="reason">${esc(t.reason)}</div></div>${approve?`<button class="btn" onclick="approveTask('${encodeURIComponent(t.task_id)}')">批准</button>`:''}</div>`}
function eventHtml(e){const msg=e.payload?.message||e.payload?.reason||JSON.stringify(e.payload||{});return `<div class="event"><div class="eventtop"><span class="eventtype">${esc(e.event_type)}</span><span class="level l${esc(e.level)}">L${esc(e.level)} · ${e.blocking?'阻塞':'提示'}</span></div><p>${esc(msg)}</p><div class="meta">回合 ${esc(e.last_seen_turn??e.turn)} · 出现 ${esc(e.seen_count||1)} 次</div></div>`}
function metric(name,value,suffix=''){return `<div class="metric"><label>${esc(name)}</label><b>${esc(value??'—')}${suffix}</b></div>`}
function render(d){$('gameStatus').innerHTML=d.game.observed?'<span class="dot ok"></span>已观测':'<span class="dot"></span>未观测';$('turn').textContent=d.game.turn??'—';$('mode').textContent=d.config.execution_mode;$('autoEnd').textContent=d.config.auto_end_turn?'开启':'关闭';$('runtime').textContent=d.server.tick_running?'执行中':'空闲';$('tickBtn').disabled=d.server.tick_running;$('waitingCount').textContent=d.waiting_tasks.length;$('taskCount').textContent=d.tasks.length;$('eventCount').textContent=d.open_events.length;$('waitingTasks').innerHTML=d.waiting_tasks.length?d.waiting_tasks.map(t=>taskHtml(t,true)).join(''):'<div class="empty">当前没有待审批任务</div>';$('allTasks').innerHTML=d.tasks.length?d.tasks.slice().reverse().slice(0,20).map(t=>taskHtml(t,false)).join(''):'<div class="empty">尚无任务记录</div>';$('events').innerHTML=d.open_events.length?d.open_events.map(eventHtml).join(''):'<div class="empty">当前没有开放事件</div>';
const a=d.latest_agent;$('agent').innerHTML=a?`<div class="metrics">${metric('回合',a.turn)}${metric('结果',a.success?'成功':'失败')}${metric('耗时',a.duration_seconds,'s')}${metric('请求体',Math.round(a.request_bytes/1024*10)/10,' KB')}</div>${a.error?`<div class="error" style="margin-top:10px">${esc(a.error)}</div>`:''}`:'<div class="empty">尚无规划调用</div>';
const m=d.latest_metrics;$('metrics').innerHTML=m?`<div class="metrics">${metric('总耗时',m.total_seconds,'s')}${metric('状态读取',m.state_query_seconds,'s')}${metric('任务执行',m.task_execution_seconds,'s')}${metric('Agent',m.agent_seconds,'s')}${metric('I/O 调用',m.mcp_call_count)}${metric('Agent 调用',m.agent_call_count)}</div>`:'<div class="empty">尚无 Tick 指标</div>';
$('errorBox').innerHTML=d.server.last_error?`<div class="error" style="margin-bottom:16px">${esc(d.server.last_error)}</div>`:''}
async function refresh(){if(!token){$('errorBox').innerHTML='<div class="error">URL 缺少访问令牌，请从启动终端复制完整地址。</div>';return}try{render(await api('/api/state'))}catch(e){$('errorBox').innerHTML=`<div class="error">${esc(e.message)}</div>`}}
async function approveTask(id){try{await api(`/api/tasks/${id}/approve`,{method:'POST',body:'{}'});await refresh()}catch(e){alert(e.message)}}
async function tick(){const b=$('tickBtn');b.disabled=true;b.textContent='执行中…';try{await api('/api/tick',{method:'POST',body:'{}'})}catch(e){alert(e.message)}finally{b.textContent='运行一次 Tick';await refresh()}}
$('refreshBtn').onclick=refresh;$('tickBtn').onclick=tick;refresh();setInterval(refresh,2000);
</script></body></html>'''
