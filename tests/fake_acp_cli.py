"""假 ACP 服务端：用 Python 脚本模拟 CodeBuddy CLI 的 stdio 行为。

这是**测试夹具**，不是产品代码。它复现的是 2026-11-10 在 CLI 2.182.0 上实测到的
报文形状（见任务目录 evidence/P0-acp-probe.md），用于在不连账号、不消耗额度的
情况下钉住传输层的契约。

用法::

    python fake_acp_cli.py --scenario happy

场景：

* ``happy``         初始化 + 建会话 + 思考块 + 三段文本 + ``end_turn``
* ``echo_prompt``   把收到的 prompt 原文当成一段正文吐回来（用于检查系统提示）
* ``echo_user``     先回吐一条 ``user_message_chunk``，再吐正文（用于检查回显过滤）
* ``auth_required`` ``session/new`` 返回 ``-32000 Authentication required``
* ``auth_flow``     ``authenticate`` 先推 authUrl 通知，0.4s 后返回
* ``malformed``     收到 ``session/new`` 后吐一行非 JSON
* ``crash``         启动即退出（退出码 3）
* ``slow``          prompt 后静默 3 秒才吐第一段（触发首帧超时）
* ``mid_error``     prompt 先吐一段，再返回 JSON-RPC 错误
* ``permission``    prompt 时发 ``session/request_permission``，必须收到回应才继续
* ``cancel``        prompt 后等待 ``session/cancel``，收到后以 ``cancelled`` 收尾
* ``stop_cancelled`` prompt 立刻以 ``cancelled`` 收尾（不等待取消通知）
* ``stop_max_tokens`` prompt 吐一段后以 ``max_tokens`` 收尾
* ``stop_refusal``  prompt 直接以 ``refusal`` 收尾（没有正文）
* ``die_mid_prompt`` prompt 吐一段后进程自杀（测 SESSION_CLOSED 收口）
* ``exit_after_answer`` 吐完正文与成功响应立刻退出（测"收尾和 EOF 撞在一起"）
* ``overflow``      收到 ``session/new`` 后吐一行超长帧
* ``big``           prompt 吐一段 2 MiB 文本（测大帧）
* ``silent``        只握手，永不回应 prompt（测超时与回收）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

_WRITE_LOCK = threading.Lock()
_CANCELLED = threading.Event()
_PERMISSION_REPLY = {}
_PERMISSION_EVENT = threading.Event()


def send(payload):
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with _WRITE_LOCK:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def ok(request_id, result):
    send({"jsonrpc": "2.0", "id": request_id, "result": result})


def fail(request_id, code, message, category=None):
    error = {"code": code, "message": message}
    if category:
        error["data"] = {"category": category}
    send({"jsonrpc": "2.0", "id": request_id, "error": error})


def notify(method, params):
    send({"jsonrpc": "2.0", "method": method, "params": params})


def chunk(session_id, text, message_id="msg-1"):
    notify(
        "session/update",
        {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": message_id,
                "content": {"type": "text", "text": text},
            },
        },
    )


def think(session_id, text):
    notify(
        "session/update",
        {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": text},
            },
        },
    )


def _delayed_echo(request_id, value, delay):
    time.sleep(delay)
    ok(request_id, {"echo": value})


def prompt_worker(scenario, session_id, request_id, prompt_text=""):
    try:
        if scenario == "echo_prompt":
            chunk(session_id, prompt_text)
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "echo_user":
            notify(
                "session/update",
                {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": {"type": "text", "text": prompt_text},
                    },
                },
            )
            chunk(session_id, "answer")
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "happy":
            think(session_id, "pondering")
            for piece in ("Hello", " world", "!"):
                chunk(session_id, piece)
                time.sleep(0.01)
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "slow":
            time.sleep(3.0)
            chunk(session_id, "late")
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "mid_error":
            chunk(session_id, "partial")
            time.sleep(0.01)
            fail(request_id, -32001, "upstream exploded")
            return

        if scenario == "die_mid_prompt":
            # 吐一段之后进程直接死掉：客户端的 SESSION_CLOSED 分支必须收口，
            # 不能挂到超时。
            chunk(session_id, "partial")
            time.sleep(0.02)
            os._exit(9)
            return

        if scenario == "exit_after_answer":
            # 正文、成功响应、EOF 三件事挤在一起。客户端先看到 SESSION_CLOSED
            # 也不能把一整条完整回答判成失败——收尾由 outcome 说了算。
            chunk(session_id, "done")
            ok(request_id, {"stopReason": "end_turn"})
            os._exit(0)
            return

        if scenario == "stop_max_tokens":
            chunk(session_id, "truncated")
            ok(request_id, {"stopReason": "max_tokens"})
            return

        if scenario == "stop_refusal":
            ok(request_id, {"stopReason": "refusal"})
            return

        if scenario == "big":
            chunk(session_id, "x" * (2 * 1024 * 1024))
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "permission":
            send(
                {
                    "jsonrpc": "2.0",
                    "id": "srv-1",
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": session_id,
                        "toolCall": {"toolCallId": "call-1", "title": "Bash"},
                        "options": [
                            {"optionId": "allow", "name": "Allow", "kind": "allow_once"}
                        ],
                    },
                }
            )
            # 必须真的收到回执，而且回执必须是"拒绝"——否则用例不能绿。
            if not _PERMISSION_EVENT.wait(5.0):
                fail(request_id, -32099, "permission reply never arrived")
                return
            outcome = (_PERMISSION_REPLY.get("result") or {}).get("outcome") or {}
            if outcome.get("outcome") != "cancelled":
                fail(request_id, -32098, "permission was not denied")
                return
            chunk(session_id, "proceeded")
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "cancel":
            chunk(session_id, "start")
            _CANCELLED.wait(5.0)
            ok(request_id, {"stopReason": "cancelled"})
            return

        if scenario == "stop_cancelled":
            ok(request_id, {"stopReason": "cancelled"})
            return

        if scenario == "thought_only":
            think(session_id, "only thought")
            ok(request_id, {"stopReason": "end_turn"})
            return

        if scenario == "silent":
            return

        # auth_flow 等场景没有 prompt 行为。
        ok(request_id, {"stopReason": "end_turn"})
    except Exception:  # noqa: BLE001 - 夹具不应因为断言失败挂住读取循环
        pass


def handle(message):
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params") or {}
    scenario = handle.scenario

    # 夹具专用的两条诊断方法，真实 CLI 没有。
    if method == "test/echo":
        delay = params.get("delay") or 0
        if delay:
            # 延时必须在后台线程里等，否则会堵住 stdin 读循环，后续请求根本
            # 发不进来，"迟到响应"这种时序就测不出来。
            threading.Thread(
                target=_delayed_echo,
                args=(request_id, params.get("n"), float(delay)),
                daemon=True,
            ).start()
        else:
            ok(request_id, {"echo": params.get("n")})
        return
    if method == "test/stats":
        ok(
            request_id,
            {
                "session_new": handle.session_new_count,
                "cancel": handle.cancel_count,
                "prompt_sessions": list(handle.prompt_sessions),
                "cancel_sessions": list(handle.cancel_sessions),
                "config_dir": os.environ.get("CODEBUDDY_CONFIG_DIR"),
                "home": os.environ.get("HOME"),
                "userprofile": os.environ.get("USERPROFILE"),
                "xdg_data": os.environ.get("XDG_DATA_HOME"),
                "xdg_config": os.environ.get("XDG_CONFIG_HOME"),
                "xdg_cache": os.environ.get("XDG_CACHE_HOME"),
                "path": os.environ.get("PATH"),
            },
        )
        return

    if method == "initialize":
        if scenario == "stray":
            # 未知 id 的响应必须被丢弃，不能误配给任何待决请求。
            send({"jsonrpc": "2.0", "id": "never-asked", "result": {"junk": True}})
        if scenario == "no_auth_methods":
            ok(
                request_id,
                {"protocolVersion": 1, "agentCapabilities": {}, "authMethods": []},
            )
            return
        ok(
            request_id,
            {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "promptCapabilities": {"image": True, "embeddedContext": True},
                    "loadSession": True,
                    "mainAgentSupport": False,
                },
                "authMethods": [
                    {"id": "iOA", "name": "Login with iOA", "description": None},
                    {"id": "internal", "name": "Login with WeChat", "description": None},
                ],
            },
        )
        return

    if method == "authenticate":
        notify(
            "_codebuddy.ai/authUrl",
            {
                "authUrl": "https://copilot.tencent.com/login?platform=CLI&state=fake-state&version=2.182.0",
                "provider": "external",
            },
        )
        time.sleep(0.4)
        ok(request_id, {})
        return

    if method == "session/new":
        if scenario == "malformed":
            with _WRITE_LOCK:
                sys.stdout.write("this is not json\n")
                sys.stdout.flush()
            return
        if scenario == "overflow":
            # 一行远超读端 limit：必须被判为协议错误，而不是截断后当两条帧。
            with _WRITE_LOCK:
                sys.stdout.write("x" * 65536 + "\n")
                sys.stdout.flush()
            return
        if scenario == "auth_required":
            fail(request_id, -32000, "Authentication required", "auth")
            return
        handle.session_new_count += 1
        # 每个会话给唯一 id：无状态路径要求「一次请求一个会话」，固定 id 会让
        # 用例无法分辨「复用了同一个」还是「真的新建了两个」。
        ok(request_id, {"sessionId": "sess-fake-{}".format(handle.session_new_count)})
        return

    if method == "session/prompt":
        session_id = params.get("sessionId") or "sess-fake-1"
        # 记下每一轮落在哪个会话上：只数 session/new 的话，"建了会话却发到旧
        # 会话上"这种回归照样能绿。
        handle.prompt_sessions.append(session_id)
        blocks = params.get("prompt") or []
        text = "".join(
            block.get("text") or ""
            for block in blocks
            if isinstance(block, dict) and block.get("type") == "text"
        )
        threading.Thread(
            target=prompt_worker,
            args=(scenario, session_id, request_id, text),
            daemon=True,
        ).start()
        return

    if method == "session/cancel":
        handle.cancel_count += 1
        handle.cancel_sessions.append(params.get("sessionId"))
        _CANCELLED.set()
        return


def handle_reply(message):
    """处理客户端对我们发出的请求的回应（目前只有 request_permission）。"""
    if message.get("id") == "srv-1":
        _PERMISSION_REPLY.update(message)
        _PERMISSION_EVENT.set()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="happy")
    args = parser.parse_args()
    handle.scenario = args.scenario
    handle.session_new_count = 0
    handle.cancel_count = 0
    handle.prompt_sessions = []
    handle.cancel_sessions = []

    if args.scenario == "crash":
        sys.exit(3)

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            message = json.loads(raw)
        except Exception:
            continue
        if not isinstance(message, dict):
            continue
        if "method" not in message:
            handle_reply(message)
            continue
        try:
            handle(message)
        except BrokenPipeError:
            return


if __name__ == "__main__":
    main()
