"""Local OpenAI-compatible HTTP gateway in front of :class:`ChatService`.

Public endpoints (all local, no upstream account data ever leaks):

* ``POST /v1/chat/completions`` -- bearer-authenticated; streams SSE or returns an
  aggregated JSON completion, exactly as the client asked;
* ``GET /v1/models`` -- bearer-authenticated; the live upstream CLI-chat catalog;
* ``GET /health`` / ``GET /healthz`` -- unauthenticated; a bare status, no
  account data.

Behaviour pinned by the contract tests:

* with an empty ``api_key`` the chat listener is never opened;
* admission is counted precisely against ``concurrency + queue_size``, a full
  queue or an expired queue wait answers 429 with ``Retry-After``, and the RPM
  budget is judged before enqueueing but spent per *real* upstream chat POST
  (shared with growth chat);
* the virtual ordered model (``core.routing.ORDERED_MODEL_ID``) is only
  advertised/served while the route is enabled; its candidates are walked by
  the real :class:`core.routing.ModelRouter`, and the virtual Chinese id is
  never forwarded upstream;
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

import aiohttp
from aiohttp import web

from .chat import aggregate_chunks
from .credits import display_model_id, upstream_model_id
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
        client,
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
        self._client = client
        self._router = router
        self._api_key = api_key or ""
        self._host = host
        self._port = port
        self._max_request_body_mb = max_request_body_mb
        self._max_request_body_bytes = max_request_body_mb * _MIB
        # The gate lives on the ChatService so a direct call and the gateway
        # share one budget; the gateway applies its configured budget to that
        # same gate (default: 1 concurrent, 6 RPM, queue 2, timeout 30s).
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

    async def _handle_models(self, request):
        if not self._authorized(request):
            return self._auth_error()
        try:
            details = await self._client.model_details()
        except UpstreamError as exc:
            return self._failed_error(exc)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            # A transport failure is a fixed JSON 502, never an unhandled 500.
            return self._failed_error(ChatStreamError("upstream request failed"))
        data = []
        for detail in details or []:
            if not isinstance(detail, dict):
                continue
            upstream_id = detail.get("id")
            if not isinstance(upstream_id, str) or not upstream_id:
                continue
            data.append(
                {
                    "id": display_model_id(detail),
                    "upstream_id": upstream_id,
                    "object": "model",
                    "created": 0,
                    "owned_by": "workbuddy",
                    "credits": detail.get("credits"),
                    "effective_credits": detail.get("effective_credits"),
                    "promo_label": detail.get("promo_label"),
                    "promo_note": detail.get("promo_note"),
                }
            )
        if self._route_enabled():
            # Appended after every live entry and without a fixed multiplier: the
            # virtual id is a route, not a price-bearing model.
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
        # Shallow-copy and remap only ``model``: a display alias becomes the pure
        # upstream id while messages / system / tools / persona / stream are left
        # untouched.  A plain id -- and the virtual id -- passes through unchanged.
        # Normalizing *once*, before the route check, is what makes the virtual
        # id's own standard display alias land on the same fixed local 400.
        payload = dict(payload)
        payload["model"] = upstream_model_id(payload["model"])
        if payload["model"] == ORDERED_MODEL_ID and not self._route_enabled():
            # The virtual id only exists with an enabled route: a fixed local 4xx
            # and never a single upstream request.
            return self._json_error(
                400,
                "the '{}' model requires an enabled routing list".format(
                    ORDERED_MODEL_ID
                ),
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
