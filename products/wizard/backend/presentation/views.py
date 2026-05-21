"""
DRF views for wizard.

Validates JSON via serializers, routes everything through the facade,
returns DTO-shaped responses. No model imports.
"""

import time
import dataclasses
from collections.abc import Iterator
from datetime import datetime
from enum import Enum
from typing import Any

from django.http import StreamingHttpResponse

import orjson
import structlog
from drf_spectacular.utils import OpenApiResponse, extend_schema
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import ValidationError
from rest_framework.renderers import BaseRenderer
from rest_framework.request import Request
from rest_framework.response import Response

from posthog.api.routing import TeamAndOrgViewSetMixin
from posthog.models.scoping import team_scope

from products.wizard.backend.facade import api as wizard_facade
from products.wizard.backend.facade.contracts import (
    UpsertWizardSessionInput,
    UpsertWizardSessionRequest,
    WizardSessionDTO,
)
from products.wizard.backend.logic.pubsub import subscribe
from products.wizard.backend.presentation.serializers import (
    UpsertWizardSessionRequestSerializer,
    WizardSessionSerializer,
)

logger = structlog.get_logger(__name__)


class EventStreamRenderer(BaseRenderer):
    """SSE pass-through renderer.

    DRF's content negotiation 406s any request whose Accept header doesn't match
    a registered renderer. EventSource sends `Accept: text/event-stream`, which
    nothing else in PostHog handles, so we register a minimal renderer that
    advertises the media type. The actual response body comes from
    StreamingHttpResponse — this renderer is never invoked to render.
    """

    media_type = "text/event-stream"
    format = "event-stream"
    charset = "utf-8"

    def render(self, data: Any, accepted_media_type: str | None = None, renderer_context: Any = None) -> bytes:
        return data if isinstance(data, bytes) else b""


def _log_request_auth(request: Request, *, action: str, team_id: int | None) -> None:
    """Info-level dump of how the incoming wizard_sessions request authenticated.

    Helps diagnose 401/403 chains by surfacing auth type, identity, available scopes,
    scoped_teams / scoped_organizations on the key, and the project the call targets.
    """
    authenticator = getattr(request, "successful_authenticator", None)
    auth_type = type(authenticator).__name__ if authenticator else "Anonymous"
    user = getattr(request, "user", None)
    user_id = getattr(user, "id", None) if user and not user.is_anonymous else None

    scopes: list[str] = []
    scoped_teams: list[int] = []
    scoped_organizations: list[str] = []

    pak = getattr(authenticator, "personal_api_key", None)
    if pak is not None:
        scopes = list(pak.scopes or [])
        scoped_teams = list(pak.scoped_teams or [])
        scoped_organizations = list(pak.scoped_organizations or [])

    token = getattr(authenticator, "access_token", None)
    if token is not None:
        scope_str: str = getattr(token, "scope", "") or ""
        scopes = list(scope_str.split())
        scoped_teams = list(getattr(token, "scoped_teams", None) or [])
        scoped_organizations = list(getattr(token, "scoped_organizations", None) or [])

    logger.info(
        "wizard_sessions request",
        action=action,
        method=request.method,
        path=request.path,
        team_id_from_url=team_id,
        auth_type=auth_type,
        user_id=user_id,
        scopes=scopes,
        scoped_teams=scoped_teams,
        scoped_organizations=scoped_organizations,
    )


SSE_HEARTBEAT_INTERVAL_SECONDS = 15.0
SSE_POLL_TIMEOUT_SECONDS = 1.0


class WizardSessionViewSet(TeamAndOrgViewSetMixin, viewsets.GenericViewSet):
    scope_object = "wizard_session"
    scope_object_read_actions = ["list", "retrieve", "stream"]
    scope_object_write_actions = ["create"]
    http_method_names = ["get", "post", "head", "options"]
    lookup_value_regex = r"[^/]+"

    def check_permissions(self, request: Request) -> None:
        """Log the auth state before DRF decides allow/deny. Fires for both 200 and 403."""
        team_id = getattr(self, "team_id", None)
        _log_request_auth(request, action=getattr(self, "action", "<unknown>"), team_id=team_id)
        super().check_permissions(request)

    @extend_schema(
        description=(
            "List wizard sessions for the project, ordered by started_at desc. "
            "Optional filters: ?workflow_id=<id> and ?skill_id=<id>."
        ),
        responses={200: WizardSessionSerializer(many=True)},
    )
    def list(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        sessions = wizard_facade.list_for_team(
            self.team_id,
            workflow_id=request.query_params.get("workflow_id"),
            skill_id=request.query_params.get("skill_id"),
        )
        page = self.paginate_queryset(sessions)
        if page is not None:
            return self.get_paginated_response(WizardSessionSerializer(page, many=True).data)
        return Response(WizardSessionSerializer(sessions, many=True).data)

    @extend_schema(
        description="Retrieve a single wizard session by its session_id (path parameter {id}).",
        responses={
            200: WizardSessionSerializer,
            404: OpenApiResponse(description="No session with that id for this project."),
        },
    )
    def retrieve(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        session_id = kwargs.get("pk")
        if not session_id:
            return Response({"detail": "session_id is required."}, status=status.HTTP_400_BAD_REQUEST)
        dto = wizard_facade.get(self.team_id, session_id)
        if dto is None:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)
        return Response(WizardSessionSerializer(dto).data)

    @extend_schema(
        description=(
            "Upsert a wizard session. The session_id key determines whether this "
            "creates a new row or replaces an existing one. Always returns 201."
        ),
        request=UpsertWizardSessionRequestSerializer,
        responses={201: WizardSessionSerializer},
    )
    def create(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        serializer = UpsertWizardSessionRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        req: UpsertWizardSessionRequest = serializer.save()

        dto = wizard_facade.upsert(
            UpsertWizardSessionInput(
                team_id=self.team_id,
                session_id=req.session_id,
                workflow_id=req.workflow_id,
                skill_id=req.skill_id,
                started_at=req.started_at,
                run_phase=req.run_phase,
                tasks=req.tasks,
                event_plan=req.event_plan,
                error=req.error,
            )
        )
        return Response(WizardSessionSerializer(dto).data, status=status.HTTP_201_CREATED)

    @extend_schema(
        description=(
            "Server-Sent Events stream of wizard session updates for a "
            "(workflow_id, skill_id) pair. On connect, the current latest "
            "session (if any) is emitted as the first event; subsequent "
            "upserts are streamed in real time."
        ),
        parameters=[
            OpenApiParameter(name="workflow_id", required=True, type=str),
            OpenApiParameter(name="skill_id", required=True, type=str),
        ],
        responses={
            (200, "text/event-stream"): {
                "type": "string",
                "description": "SSE stream of WizardSession events.",
            }
        },
    )
    @action(detail=False, methods=["get"], url_path="stream", renderer_classes=[EventStreamRenderer])
    def stream(self, request: Request, *args: Any, **kwargs: Any) -> StreamingHttpResponse:
        workflow_id = request.query_params.get("workflow_id")
        skill_id = request.query_params.get("skill_id") or None
        if not workflow_id:
            raise ValidationError({"detail": "workflow_id is required."})

        generator = _wizard_session_event_stream(
            team_id=self.team_id,
            workflow_id=workflow_id,
            skill_id=skill_id,
        )
        return StreamingHttpResponse(
            generator,
            status=status.HTTP_200_OK,
            content_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )


def _wizard_session_event_stream(team_id: int, workflow_id: str, skill_id: str | None = None) -> Iterator[bytes]:
    """Yield SSE-formatted bytes for a wizard session subscription.

    If `skill_id` is provided, scope to that exact pair. Otherwise pattern-
    subscribe to all skills under (team, workflow_id).

    Emits the current latest session (if any) first, then forwards Redis
    pub/sub messages. Yields heartbeat comments roughly every
    SSE_HEARTBEAT_INTERVAL_SECONDS so proxies don't time the connection out.

    Note: streaming generators run after the view returns, on a sync worker
    thread that no longer has the request's team-scope thread-local set.
    The fail-closed manager on `WizardSession` would refuse any DB query
    here, so we re-establish team_scope explicitly for any DB access.
    """
    with team_scope(team_id):
        latest = wizard_facade.get_latest(team_id, workflow_id, skill_id)
    if latest is not None:
        yield _format_event(latest)

    with subscribe(team_id, workflow_id, skill_id) as pubsub:
        last_heartbeat = time.monotonic()
        while True:
            message = pubsub.get_message(timeout=SSE_POLL_TIMEOUT_SECONDS)
            now = time.monotonic()

            # `message` type is direct-channel; `pmessage` is pattern subscribe.
            if message and message.get("type") in ("message", "pmessage"):
                yield b"data: " + message["data"] + b"\n\n"
                last_heartbeat = now
                continue

            if now - last_heartbeat >= SSE_HEARTBEAT_INTERVAL_SECONDS:
                yield b": ping\n\n"
                last_heartbeat = now


def _format_event(dto: WizardSessionDTO) -> bytes:
    payload = orjson.dumps(dto, default=_json_default)
    return b"data: " + payload + b"\n\n"


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
