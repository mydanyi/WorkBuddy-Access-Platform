"""Local OpenAI-compatible HTTP gateway in front of the chat backend.

Public endpoints (all local, no upstream account data ever leaks):

* ``POST /v1/chat/completions`` -- bearer-authenticated; streams SSE or returns an
  aggregated JSON completion, exactly as the client asked;
* ``GET /v1/models`` -- bearer-authenticated; the model list this install can
  actually serve, read from the local CLI (never from an upstream account API);
* ``GET /health`` / ``GET /healthz`` -- unauthenticated; a bare status, no
  account data.

Behaviour pinned by the contract tests:

* with an empty ``api_key`` the chat listener is never opened;
* admission is counted precisely against ``concurrency + queue_size``, a full
  queue or an expired queue wait answers 429 with ``Retry-After``, and the RPM
  budget is judged before enqueueing but spent per *real* ACP prompt;
* the virtual ordered model (``core.routing.ORDERED_MODEL_ID``) is only
  advertised/served while the route is enabled; its candidates are walked by
  the real :class:`core.routing.ModelRouter`, and the virtual Chinese id is
  never forwarded to a CLI process;
* a model id that the local catalog does not contain is a fixed local 400 (never
  a spawned process that would fail later with a 502); when the catalog is empty
  -- nothing has ever been read -- the check is skipped rather than refusing
  every request;
* the request body is capped by ``gateway.max_request_body_mb`` (default 16
  MiB) in real UTF-8 bytes -- Content-Length and chunked bodies alike -- with an
  OpenAI-compatible JSON ``413 request_too_large`` that echoes no request data;
* the streamed response is prepared only once the first valid frame exists, so a
  pre-stream failure still yields a real HTTP error; a mid-stream failure emits
  an OpenAI ``error`` event and never a fabricated ``[DONE]``;
* the upstream is never replayed automatically after the stream has started;
* a local :class:`core.limits.LimitExceeded` is always a 429, never a 502, and
  never turns into a standby attempt;
* ``stop`` cancels queued waiters and in-flight handlers, then cleans the runner.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import math

from aiohttp import web
from .chat import aggregate_chunks
from .errors import ChatStreamError, UpstreamError
from .limits import ConcurrencyGate, LimitExceeded
from .routing import ORDERED_MODEL_ID

COMPLETIONS_PATH = "/v1/chat/completions"
MODELS_PATH = "/v1/models"
HEALTH_PATH = "/health"
HEALTHZ_PATH = "/healthz"

_MIB = 1024 * 1024
# Approved default body cap; ``0`` never means "unlimited".
_DEFAULT_MAX_REQUEST_BODY_MB = 16

# The router's own fixed code for "no candidate may be asked this round": that
# refusal is a *local* 429 rate limit, never a plain upstream failure.
_ROUTING_COOLING_CODE = "routing_cooling"

_SSE_HEADERS = {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
}


async def _aclose(agen) -> None:
    with contextlib.suppress(Exception):
        await agen.aclose()


def _with_candidate_model(chunk, model):
    """Fill a missing ``model`` on a routed frame with its known candidate id.

    A frame that already carries a usable ``model`` (and every non-dict frame) is
    returned untouched, so the upstream's own metadata always wins.
    """
    if not isinstance(chunk, dict):
        return chunk
    if not isinstance(model, str) or not model:
        return chunk
    if chunk.get("model") not in (None, ""):
        return chunk
    tagged = dict(chunk)
    tagged["model"] = model
    return tagged


class Gateway:
    """Hosts the OpenAI-compatible endpoints and owns the shared gate."""

    def __init__(
        self,
        chat,
        *,
        api_key,
        host="127.0.0.1",
        port=0,
        concurrency=1,
        rpm=6,
        queue_size=2,
        queue_timeout=30.0,
        limits=None,
        router=None,
        catalog=None,
        max_request_body_mb=_DEFAULT_MAX_REQUEST_BODY_MB,
    ):
        # The body cap is a positive integer number of MiB: ``bool``, ``0``, a
        # negative value, a float or a string is a programming error, never a
        # silent "unlimited".
        if (
            isinstance(max_request_body_mb, bool)
            or not isinstance(max_request_body_mb, int)
            or max_request_body_mb <= 0
        ):
            raise ValueError("max_request_body_mb must be a positive integer")
        self._chat = chat
        # 模型清单的来源。给 ``None`` 表示"这个网关不掌握清单"：``/v1/models``
        # 就只报虚拟入口，聊天也不做未知模型校验。生产里一定是
        # :class:`core.models.ModelCatalog`。
        self._catalog = catalog
        self._router = router
        self._api_key = api_key or ""
        self._host = host
        self._port = port
        self._max_request_body_mb = max_request_body_mb
        self._max_request_body_bytes = max_request_body_mb * _MIB
        # 并发门挂在**聊天后端**上，而不是网关自己：直连后端和走网关用的是同一个
        # 预算。网关把自己配置的那份写上去（默认 1 并发、6 RPM、队列 2、等 30 秒）。
        if limits is not None:
            chat.limits = limits
        else:
            chat.limits = ConcurrencyGate(
                concurrency=concurrency,
                queue_size=queue_size,
                queue_timeout=queue_timeout,
                rpm=rpm,
            )
        self.limits = chat.limits
        self._runner = None
        self._site = None
        self.bound_port = None
        self._inflight = set()
        self._stopping = False
        # aiohttp raises once the *streamed* body reaches ``client_max_size``, so
        # one byte of slack keeps a body of exactly the approved size acceptable
        # and refuses only a genuinely larger one; the handler's own checks stay
        # byte-exact against ``max_request_body_bytes``.
        self._app = web.Application(
            middlewares=[self._track_inflight],
            client_max_size=self._max_request_body_bytes + 1,
        )
        self._app.router.add_post(COMPLETIONS_PATH, self._handle_chat)
        self._app.router.add_get(MODELS_PATH, self._handle_models)
        self._app.router.add_get(HEALTH_PATH, self._handle_health)
        self._app.router.add_get(HEALTHZ_PATH, self._handle_health)

    # -- lifecycle -----------------------------------------------------------

    @web.middleware
    async def _track_inflight(self, request, handler):
        task = asyncio.current_task()
        if task is not None:
            self._inflight.add(task)
        try:
            return await handler(request)
        finally:
            if task is not None:
                self._inflight.discard(task)

    async def start(self) -> None:
        """Bind the listener unless no API key is configured (then never listen)."""
        if not self._api_key:
            self.bound_port = None
            return
        # ``handler_cancellation`` is an ``AppRunner`` argument, not an
        # ``Application`` one: it lets a client disconnect cancel the handler.
        self._runner = web.AppRunner(self._app, handler_cancellation=True)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()
        address = self._runner.addresses[0]
        self.bound_port = int(address[1])

    async def stop(self) -> None:
        """Refuse new work, cancel + await in-flight handlers, clean the runner."""
        self._stopping = True
        await self.limits.close()
        current = asyncio.current_task()
        tasks = [t for t in list(self._inflight) if t is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._runner is not None:
            with contextlib.suppress(Exception):
                await self._runner.cleanup()
            self._runner = None
            self._site = None
        # No bound port means "no longer listening".
        self.bound_port = None

    # -- auth / responses ----------------------------------------------------

    def _authorized(self, request) -> bool:
        if not self._api_key:
            return False
        scheme, _, token = request.headers.get("Authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not token:
            return False
        # Compare as UTF-8 bytes: a wrong non-ASCII bearer (e.g. a Latin-1 'é')
        # must be a clean 401, never a 500 from ``compare_digest`` on non-ASCII
        # ``str`` inputs.
        return hmac.compare_digest(
            token.encode("utf-8"), self._api_key.encode("utf-8")
        )

    def _json_error(self, status, message, err_type, retry_after=None, *, code=None):
        """The one JSON error shape; ``retry_after`` stays the 4th positional.

        ``code`` is keyword-only so every existing call site keeps its meaning;
        only a 413 sets it, everything else keeps the historic ``null``.
        """
        response = web.json_response(
            {"error": {"message": message, "type": err_type, "code": code}},
            status=status,
        )
        if retry_after:
            response.headers["Retry-After"] = str(
                max(1, int(math.ceil(float(retry_after))))
            )
        return response

    def _auth_error(self):
        return self._json_error(401, "invalid or missing API key", "invalid_request_error")

    def _limited_error(self, exc):
        return self._json_error(
            429,
            "rate limit exceeded: " + exc.reason,
            "rate_limit_error",
            exc.retry_after,
        )

    def _too_large_error(self):
        """The fixed JSON 413: states the limit and the knob, echoes no body."""
        return self._json_error(
            413,
            "请求体超过上限 {} MiB（gateway.max_request_body_mb）；"
            "请缩减历史消息、工具定义或图片内容，或调整该配置后重试。".format(
                self._max_request_body_mb
            ),
            "request_too_large",
            code="request_too_large",
        )

    def _failed_error(self, exc):
        if isinstance(exc, UpstreamError):
            status = exc.status if isinstance(exc.status, int) and exc.status >= 400 else 502
        else:
            status = getattr(exc, "status", 502) or 502
        # Only the router's own 429 cooling refusal reads as a rate limit (HTTP
        # 429 and its Retry-After are kept); every other upstream failure keeps
        # the historic ``upstream_error`` type and is never a local limit.
        err_type = "upstream_error"
        if (
            isinstance(exc, UpstreamError)
            and status == 429
            and getattr(exc, "code", None) == _ROUTING_COOLING_CODE
        ):
            err_type = "rate_limit_error"
        return self._json_error(
            status,
            "upstream request failed",
            err_type,
            getattr(exc, "retry_after", None),
        )

    # -- routing helpers -----------------------------------------------------

    def _route_enabled(self) -> bool:
        """True only for a real router whose enabled route is configured."""
        router = self._router
        if router is None:
            return False
        try:
            status = router.status()
        except Exception:
            return False
        return isinstance(status, dict) and status.get("enabled") is True

    async def _candidate_stream(self, payload):
        """One routed attempt: the real chat stream, muted for the observer.

        The router hands back only a shallow copy with a real candidate ``model``,
        so the payload itself is forwarded untouched and exactly one admission
        slot (already held by the caller) is used -- a standby attempt never
        takes a second one.
        """
        model = payload.get("model") if isinstance(payload, dict) else None
        agen = self._chat.stream(payload, observe_errors=False)
        try:
            async for chunk in agen:
                yield _with_candidate_model(chunk, model)
        finally:
            await _aclose(agen)

    def _declared_too_large(self, request) -> bool:
        """The declared Content-Length is over the cap (no body byte read yet)."""
        length = request.content_length
        return (
            isinstance(length, int)
            and not isinstance(length, bool)
            and length > self._max_request_body_bytes
        )

    # -- handlers ------------------------------------------------------------

    async def _handle_health(self, request):
        return web.json_response({"status": "ok"})

    async def _catalog_models(self) -> list:
        """取本地模型清单；读不到就当空列表，绝不编一份出来。

        ``ensure`` 自带保鲜期，所以这里不会每来一次请求就 spawn 一个 ``--help``。
        读失败是"这台机器上没装 CLI / 还没读过"，只影响本接口与未知模型校验，不该
        让 ``/v1/models`` 变成一个 5xx。
        """
        catalog = self._catalog
        if catalog is None:
            return []
        try:
            models = await catalog.ensure()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 清单读不出来不是网关故障
            return []
        if not isinstance(models, list):
            return []
        return [model for model in models if isinstance(model, str) and model]

    def _known_model(self, model, models) -> bool:
        """该模型能不能交给 CLI。

        清单为空时一律放行：那说明这台机器还没成功读过清单（CLI 没装、装晚了一步），
        此时拒绝所有请求只会把"还没准备好"变成"永远不能用"。清单非空才做校验——
        此时一个不认识的 id 意味着 CLI 那边一定会失败，早点回 400 比让它变成一条
        注定失败的进程 + 一条"上游异常"提醒好。
        """
        if not models:
            return True
        return model in models

    async def _handle_models(self, request):
        if not self._authorized(request):
            return self._auth_error()
        models = await self._catalog_models()
        data = [
            {
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "workbuddy",
            }
            for model in models
        ]
        if self._route_enabled():
            # Appended after every real entry: the virtual id is a route, never a
            # model the CLI knows how to start.
            data.append(
                {
                    "id": ORDERED_MODEL_ID,
                    "object": "model",
                    "created": 0,
                    "owned_by": "workbuddy",
                }
            )
        return web.json_response({"object": "list", "data": data})

    async def _handle_chat(self, request):
        # Authorization and the shutdown flag are judged before a single body
        # byte is read.
        if not self._authorized(request):
            return self._auth_error()
        if self._stopping:
            return self._json_error(503, "gateway is shutting down", "server_error")
        if self._declared_too_large(request):
            # Refused before admission and before the route: no slot, no upstream.
            return self._too_large_error()
        try:
            payload = await request.json()
        except web.HTTPRequestEntityTooLarge:
            # aiohttp raised on the real streamed byte count (chunked / multibyte
            # included): answer the same fixed JSON 413 instead of a bare page.
            return self._too_large_error()
        except Exception:
            return self._json_error(
                400, "request body must be valid JSON", "invalid_request_error"
            )
        if not isinstance(payload, dict):
            return self._json_error(
                400, "request body must be a JSON object", "invalid_request_error"
            )
        messages = payload.get("messages")
        if not payload.get("model") or not isinstance(messages, list) or not messages:
            return self._json_error(
                400,
                "both 'model' and a non-empty 'messages' list are required",
                "invalid_request_error",
            )
        # A plain id passes through untouched: the model *is* the process's
        # ``--model`` argument, so there is nothing to translate or normalize.
        payload = dict(payload)
        model = payload["model"]
        if model == ORDERED_MODEL_ID:
            if not self._route_enabled():
                # The virtual id only exists with an enabled route: a fixed local
                # 4xx and never a single CLI process.
                return self._json_error(
                    400,
                    "the '{}' model requires an enabled routing list".format(
                        ORDERED_MODEL_ID
                    ),
                    "invalid_request_error",
                )
        elif not self._known_model(model, await self._catalog_models()):
            return self._json_error(
                400,
                "unknown model '{}': check /v1/models for the ids this install "
                "can serve".format(model),
                "invalid_request_error",
            )
        stream = bool(payload.get("stream"))
        try:
            # The rpm permit is only *judged* here; the chat layer spends it per
            # real upstream POST, so a failover never needs a second slot.
            async with self.limits.slot(count_rpm=False):
                return await self._serve(payload, stream, request)
        except LimitExceeded as exc:
            return self._limited_error(exc)

    async def _serve(self, payload, stream, request):
        if payload.get("model") == ORDERED_MODEL_ID:
            # The virtual id is walked by the real router; ``send`` opens exactly
            # one candidate stream inside the slot we already hold.
            agen = self._router.stream(payload, self._candidate_stream)
        else:
            agen = self._chat.stream(payload)
        if not stream:
            chunks = []
            try:
                async for chunk in agen:
                    chunks.append(chunk)
                return web.json_response(aggregate_chunks(chunks, payload))
            except LimitExceeded:
                # A local admission refusal is the caller's 429, never a 502 and
                # never a reason to try a standby model.
                raise
            except (UpstreamError, ChatStreamError) as exc:
                return self._failed_error(exc)
            except Exception:
                return self._failed_error(ChatStreamError("upstream chat request failed"))
            finally:
                await _aclose(agen)

        # Fetch the first frame before preparing: a pre-stream failure can still
        # answer with a real HTTP status instead of a half-written SSE body.
        try:
            first = await agen.__anext__()
        except StopAsyncIteration:
            await _aclose(agen)
            return self._failed_error(ChatStreamError("upstream returned an empty stream"))
        except LimitExceeded:
            await _aclose(agen)
            raise
        except (UpstreamError, ChatStreamError) as exc:
            await _aclose(agen)
            return self._failed_error(exc)
        except Exception:
            await _aclose(agen)
            return self._failed_error(ChatStreamError("upstream chat request failed"))
        except BaseException:
            await _aclose(agen)
            raise

        response = web.StreamResponse(status=200, headers=dict(_SSE_HEADERS))
        completed = False
        # ``prepare`` and every write live inside the try/finally so a client
        # disconnect or a failed ``prepare`` still closes the upstream generator.
        try:
            await response.prepare(request)
            await self._write_frame(response, first)
            async for chunk in agen:
                await self._write_frame(response, chunk)
            completed = True
        except (UpstreamError, ChatStreamError) as exc:
            # Mid-stream failure: an OpenAI error event, never a fake [DONE].
            with contextlib.suppress(Exception):
                await self._write_error_event(response, exc)
        finally:
            await _aclose(agen)
            if completed:
                with contextlib.suppress(Exception):
                    await response.write(b"data: [DONE]\n\n")
                with contextlib.suppress(Exception):
                    await response.write_eof()
        return response

    # -- frame writers -------------------------------------------------------

    async def _write_frame(self, response, chunk) -> None:
        data = json.dumps(chunk, ensure_ascii=False, separators=(",", ":"))
        await response.write(("data: " + data + "\n\n").encode("utf-8"))

    async def _write_error_event(self, response, exc) -> None:
        payload = {
            "error": {
                "message": str(exc),
                "type": "upstream_error",
                "code": getattr(exc, "code", None),
            }
        }
        data = json.dumps(payload, ensure_ascii=False)
        await response.write(("data: " + data + "\n\n").encode("utf-8"))
