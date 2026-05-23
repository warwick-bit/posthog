"""
Facade API for user_interviews.

This is the ONLY module other apps are allowed to import.

Responsibilities:
- Accept primitives / contracts as input
- Call domain logic / ORM
- Return contracts (no ORM instances)
- Remain thin and stable

Do NOT:
- Import DRF, serializers, or HTTP concerns
- Return ORM instances or QuerySets
"""

from typing import Optional
from uuid import UUID

from posthog.models.sharing_configuration import SharingConfiguration

from products.user_interviews.backend import logic
from products.user_interviews.backend.facade.contracts import IntervieweeIdentity
from products.user_interviews.backend.max_tools import (
    AnalyzeUserInterviewsTool,
    CreateUserInterviewTopicTool,
    GenerateTestInterviewLinkTool,
)
from products.user_interviews.backend.models import IntervieweeContext, UserInterviewTopic

__all__ = [
    "AnalyzeUserInterviewsTool",
    "CreateUserInterviewTopicTool",
    "GenerateTestInterviewLinkTool",
    "IntervieweeIdentity",
    "build_test_interview_share",
    "has_replied",
    "parse_interviewee_identifier",
]


def parse_interviewee_identifier(identifier: str) -> IntervieweeIdentity:
    return logic.parse_interviewee_identifier(identifier)


def has_replied(*, team_id: int, topic_id: UUID, interviewee_identifier: str) -> bool:
    return logic.has_replied(
        team_id=team_id,
        topic_id=topic_id,
        interviewee_identifier=interviewee_identifier,
    )


def build_test_interview_share(access_token: str) -> Optional[SharingConfiguration]:
    """Resolve a synthetic-test-interviewee access token (``test-<topic_uuid>``) to an
    unsaved ``SharingConfiguration`` that points at the topic via an unsaved
    ``IntervieweeContext``. Returns ``None`` for any other token shape.

    No DB rows exist for the synthetic test interviewee — the URL is fully derivable
    from the topic UUID. Constructing the in-memory objects here lets the existing
    interview-rendering branch (which expects ``SharingConfiguration.interviewee_context.topic``)
    handle this case unchanged. The objects must never be saved.
    """
    if not access_token.startswith(logic.TEST_INTERVIEW_TOKEN_PREFIX):
        return None
    topic_uuid = access_token[len(logic.TEST_INTERVIEW_TOKEN_PREFIX) :]
    # Looking up by `id` alone (without a `team_id=...` clause) is intentional and
    # parallels how `SharingConfiguration` is fetched by `access_token`: the topic UUID
    # is itself the unguessable public token for the synthetic test interviewee, so
    # requiring a separate team filter would not add a security boundary.
    try:
        topic = UserInterviewTopic.objects.select_related(  # nosemgrep: semgrep.rules.idor-lookup-without-team
            "team", "team__organization", "created_by"
        ).get(id=topic_uuid)
    except (ValueError, UserInterviewTopic.DoesNotExist):
        return None
    interviewee_context = IntervieweeContext(
        team=topic.team,
        topic=topic,
        interviewee_identifier=logic.TEST_INTERVIEWEE_DISPLAY_NAME,
        agent_context="",
    )
    sharing_config = SharingConfiguration(team=topic.team, enabled=True, access_token=access_token)
    # Bypass Django's FK descriptor "not saved" check by writing directly into the
    # forward-relation cache. We never call `.save()` on either side — these objects
    # exist only so the rendering branch downstream can dereference
    # `sharing_config.interviewee_context.topic` without a DB lookup.
    SharingConfiguration._meta.get_field("interviewee_context").set_cached_value(sharing_config, interviewee_context)
    return sharing_config
