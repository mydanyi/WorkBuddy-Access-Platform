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
  budget is judged before enqueueing (and shared with future growth chat);
* the streamed response is prepared only once the first valid frame exists, so a
  pre-stream failure still yields a real HTTP error; a mid-stream failure emits
  an OpenAI ``error`` event and never a fabricated ``[DONE]``;
* the upstream is never replayed automatically after the stream has started;
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

COMPLETIONS_PATH = "/v1/chat/completions"
MODELS_PATH = "/v1/models"
HEALTH_PATH = "/health"
HEALTHZ_PATH = "/healthz"

_SSE_HEADERS = {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
}


async def _aclose(agen) -> None:
    with contextlib.suppress(Exception):
        await agen.aclose()


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
    ):
        self._chat = chat
        self._client = client
        self._api_key = api_key or ""
        self._host = host
        self._port = port
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
        self._app = web.Application(middlewares=[self._track_inflight])
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

    def _json_error(self, status, message, err_type, retry_after=None):
        response = web.json_response(
            {"error": {"message": message, "type": err_type, "code": None}},
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

    def _failed_error(self, exc):
        if isinstance(exc, UpstreamError):
            status = exc.status if isinstance(exc.status, int) and exc.status >= 400 else 502
        else:
            status = getattr(exc, "status", 502) or 502
        return self._json_error(
            status,
            "upstream request failed",
            "upstream_error",
            getattr(exc, "retry_after", None),
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
        return web.json_response({"object": "list", "data": data})

    async def _handle_chat(self, request):
        if not self._authorized(request):
            return self._auth_error()
        if self._stopping:
            return self._json_error(503, "gateway is shutting down", "server_error")
        try:
            payload = await request.json()
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
        # untouched.  A plain id passes through unchanged.
        payload = dict(payload)
        payload["model"] = upstream_model_id(payload["model"])
        stream = bool(payload.get("stream"))
        try:
            async with self.limits.slot():
                return await self._serve(payload, stream, request)
        except LimitExceeded as exc:
            return self._limited_error(exc)

    async def _serve(self, payload, stream, request):
        agen = self._chat.stream(payload)
        if not stream:
            chunks = []
            try:
                async for chunk in agen:
                    chunks.append(chunk)
                return web.json_response(aggregate_chunks(chunks, payload))
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
