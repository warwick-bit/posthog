import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Optional

import structlog
from prometheus_client import Counter

from posthog.hogql import ast

from posthog.clickhouse.client import sync_execute
from posthog.clickhouse.preaggregation.web_overview_preaggregated_sql import (
    DISTRIBUTED_WEB_OVERVIEW_PREAGGREGATED_TABLE,
)
from posthog.clickhouse.query_tagging import Feature, Product, tag_queries

from products.analytics_platform.backend.lazy_computation.lazy_computation_executor import (
    LazyComputationResult,
    LazyComputationTable,
    ensure_precomputed,
)
from products.web_analytics.backend.hogql_queries.web_lazy_precompute_common import (
    LAZY_TTL_SECONDS,
    SESSION_FORWARD_PAD_MINUTES,
    LazyPrecomputeIneligible,
    ceil_utc_day,
    check_common_eligibility,
    floor_utc_day,
    host_filter_expr,
    log_eligibility_outcome,
    test_account_filter_expr,
)

if TYPE_CHECKING:
    from products.web_analytics.backend.hogql_queries.web_overview import WebOverviewQueryRunner

logger = structlog.get_logger(__name__)


WEB_OVERVIEW_LAZY_FAILED = Counter(
    "web_overview_lazy_precompute_failed_total",
    "Lazy precompute path failures, by error class",
    ["error_type"],
)


def can_use_lazy_precompute(runner: "WebOverviewQueryRunner") -> bool:
    """Return True iff the lazy precompute path is eligible. Logs rejection
    reason at INFO level so we can attribute every fall-through after deploy."""
    try:
        _check_eligible(runner)
    except LazyPrecomputeIneligible as exc:
        log_eligibility_outcome(log_prefix="web_overview_lazy_precompute", team_id=runner.team.pk, error=exc)
        return False
    log_eligibility_outcome(log_prefix="web_overview_lazy_precompute", team_id=runner.team.pk, error=None)
    return True


def _check_eligible(runner: "WebOverviewQueryRunner") -> None:
    query = runner.query
    check_common_eligibility(
        team=runner.team,
        use_web_analytics_precompute=query.useWebAnalyticsPrecompute,
        conversion_goal=query.conversionGoal,
        sampling=query.sampling,
        modifiers=query.modifiers,
        properties=query.properties or [],
        resolve_date_range=lambda: (runner.query_date_range.date_from(), runner.query_date_range.date_to()),
    )


def _events_session_id_expr(runner: "WebOverviewQueryRunner") -> ast.Expr:
    return runner.events_session_property


# HogQL template for the precompute INSERT. The lazy_computation framework
# substitutes the listed placeholders (including `time_window_min`/`time_window_max`),
# parses the result, and INSERTs into `web_overview_preaggregated`. The framework
# automatically prepends `team_id`, `job_id` and appends `expires_at` to the SELECT.
INSERT_QUERY_TEMPLATE = """
SELECT
    toStartOfHour(start_timestamp) AS time_window_start,
    uniqState(session_person_id) AS uniq_users_state,
    uniqState(session_id) AS uniq_sessions_state,
    sumState(assumeNotNull(toInt(filtered_pageview_count))) AS sum_pageviews_state,
    avgState(assumeNotNull(toFloat(session_duration))) AS avg_duration_state,
    avgState(assumeNotNull(toInt(is_bounce))) AS avg_bounce_state
FROM (
    SELECT
        any(events.person_id) AS session_person_id,
        {events_session_id} AS session_id,
        min(session.$start_timestamp) AS start_timestamp,
        any(session.$session_duration) AS session_duration,
        countIf(or(equals(event, '$pageview'), equals(event, '$screen'))) AS filtered_pageview_count,
        any(session.$is_bounce) AS is_bounce
    FROM events
    WHERE and(
        {events_session_id} IS NOT NULL,
        {event_type_filter},
        timestamp >= {time_window_min},
        timestamp < ({time_window_max} + toIntervalMinute({pad_minutes})),
        {user_filter},
        {test_account_filter}
    )
    GROUP BY session_id
    HAVING and(
        toStartOfHour(min(session.$start_timestamp)) >= {time_window_min},
        toStartOfHour(min(session.$start_timestamp)) < {time_window_max}
    )
)
GROUP BY time_window_start
"""


def ensure_web_overview_precomputed(
    runner: "WebOverviewQueryRunner",
    time_range_start: datetime,
    time_range_end: datetime,
) -> LazyComputationResult:
    placeholders: dict[str, ast.Expr] = {
        "events_session_id": _events_session_id_expr(runner),
        "event_type_filter": runner.event_type_expr,
        "user_filter": host_filter_expr(runner.query.properties or []),
        "test_account_filter": test_account_filter_expr(
            test_account_filters=runner._test_account_filters, team=runner.team
        ),
        "pad_minutes": ast.Constant(value=SESSION_FORWARD_PAD_MINUTES),
    }

    return ensure_precomputed(
        team=runner.team,
        insert_query=INSERT_QUERY_TEMPLATE,
        time_range_start=time_range_start,
        time_range_end=time_range_end,
        ttl_seconds=LAZY_TTL_SECONDS,
        table=LazyComputationTable.WEB_OVERVIEW_PREAGGREGATED,
        placeholders=placeholders,
        query_type="web_overview_lazy_insert",
    )


_READ_SQL = f"""
SELECT
    uniqMergeIf(uniq_users_state, time_window_start >= %(cur_start)s AND time_window_start < %(cur_end)s) AS unique_users,
    uniqMergeIf(uniq_users_state, time_window_start >= %(prev_start)s AND time_window_start < %(prev_end)s) AS previous_unique_users,
    sumMergeIf(sum_pageviews_state, time_window_start >= %(cur_start)s AND time_window_start < %(cur_end)s) AS views,
    sumMergeIf(sum_pageviews_state, time_window_start >= %(prev_start)s AND time_window_start < %(prev_end)s) AS previous_views,
    uniqMergeIf(uniq_sessions_state, time_window_start >= %(cur_start)s AND time_window_start < %(cur_end)s) AS sessions,
    uniqMergeIf(uniq_sessions_state, time_window_start >= %(prev_start)s AND time_window_start < %(prev_end)s) AS previous_sessions,
    avgMergeIf(avg_duration_state, time_window_start >= %(cur_start)s AND time_window_start < %(cur_end)s) AS avg_duration,
    avgMergeIf(avg_duration_state, time_window_start >= %(prev_start)s AND time_window_start < %(prev_end)s) AS previous_avg_duration,
    avgMergeIf(avg_bounce_state, time_window_start >= %(cur_start)s AND time_window_start < %(cur_end)s) AS bounce_rate,
    avgMergeIf(avg_bounce_state, time_window_start >= %(prev_start)s AND time_window_start < %(prev_end)s) AS previous_bounce_rate
FROM {DISTRIBUTED_WEB_OVERVIEW_PREAGGREGATED_TABLE()}
WHERE team_id = %(team_id)s AND job_id IN %(job_ids)s
"""


_READ_SETTINGS = {
    # Approach E from `products/analytics_platform/backend/lazy_computation/CONSISTENCY.md`.
    "load_balancing": "in_order",
    "optimize_skip_unused_shards": 1,
}


def execute_read_query(
    *,
    team_id: int,
    job_ids: list[str],
    current_start_utc: datetime,
    current_end_utc: datetime,
    previous_start_utc: Optional[datetime],
    previous_end_utc: Optional[datetime],
) -> list:
    """Run the precompute-read SQL via `sync_execute`."""
    prev_start = previous_start_utc if previous_start_utc is not None else datetime(1970, 1, 1, tzinfo=UTC)
    prev_end = previous_end_utc if previous_end_utc is not None else datetime(1970, 1, 1, tzinfo=UTC)

    tag_queries(product=Product.WEB_ANALYTICS, feature=Feature.QUERY, query_type="web_overview_lazy_query")
    return sync_execute(
        _READ_SQL,
        {
            "team_id": team_id,
            "job_ids": tuple(str(jid) for jid in job_ids),
            "cur_start": current_start_utc,
            "cur_end": current_end_utc,
            "prev_start": prev_start,
            "prev_end": prev_end,
        },
        settings=_READ_SETTINGS,
        team_id=team_id,
    )


def _empty_response_row() -> list:
    return [0, None, 0, None, 0, None, 0, None, 0, None]


def execute_lazy_precomputed_read(
    runner: "WebOverviewQueryRunner",
) -> Optional[list]:
    """Orchestrate the lazy precompute + read. Returns the response row, or None
    on any failure (caller falls through to the v2/raw path)."""
    tag_queries(product=Product.WEB_ANALYTICS, feature=Feature.QUERY)
    team_id = runner.team.pk
    overall_started = time.perf_counter()
    try:
        date_from = runner.query_date_range.date_from()
        date_to = runner.query_date_range.date_to()
        assert date_from is not None and date_to is not None

        current_start_utc = date_from.astimezone(UTC)
        current_end_utc = date_to.astimezone(UTC)

        time_range_start = floor_utc_day(current_start_utc)
        time_range_end = ceil_utc_day(current_end_utc)

        if time_range_start >= time_range_end:
            logger.info(
                "web_overview_lazy_precompute_empty_range",
                team_id=team_id,
                time_range_start=time_range_start.isoformat(),
                time_range_end=time_range_end.isoformat(),
            )
            return None

        logger.info(
            "web_overview_lazy_precompute_started",
            team_id=team_id,
            time_range_start=time_range_start.isoformat(),
            time_range_end=time_range_end.isoformat(),
            time_range_days=(time_range_end - time_range_start).days,
        )

        ensure_started = time.perf_counter()
        result = ensure_web_overview_precomputed(
            runner=runner,
            time_range_start=time_range_start,
            time_range_end=time_range_end,
        )
        ensure_duration_ms = int((time.perf_counter() - ensure_started) * 1000)

        logger.info(
            "web_overview_lazy_precompute_ensure_done",
            team_id=team_id,
            job_count=len(result.job_ids),
            ensure_duration_ms=ensure_duration_ms,
        )

        if not result.job_ids:
            return None

        if not result.ready:
            logger.info(
                "web_overview_lazy_precompute_current_not_ready",
                team_id=team_id,
                job_count=len(result.job_ids),
            )
            return None

        job_ids: list[str] = [str(jid) for jid in result.job_ids]

        previous_start_utc: Optional[datetime] = None
        previous_end_utc: Optional[datetime] = None
        if runner.query_compare_to_date_range is not None:
            prev_from = runner.query_compare_to_date_range.date_from()
            prev_to = runner.query_compare_to_date_range.date_to()
            if prev_from is not None and prev_to is not None:
                previous_start_utc = prev_from.astimezone(UTC)
                previous_end_utc = prev_to.astimezone(UTC)

                prev_range_start = floor_utc_day(previous_start_utc)
                prev_range_end = ceil_utc_day(previous_end_utc)
                if prev_range_start < prev_range_end:
                    prev_ensure_started = time.perf_counter()
                    prev_result = ensure_web_overview_precomputed(
                        runner=runner,
                        time_range_start=prev_range_start,
                        time_range_end=prev_range_end,
                    )
                    ensure_duration_ms += int((time.perf_counter() - prev_ensure_started) * 1000)

                    if not prev_result.ready:
                        logger.info(
                            "web_overview_lazy_precompute_previous_not_ready",
                            team_id=team_id,
                            prev_job_count=len(prev_result.job_ids),
                        )
                        return None

                    job_ids.extend(str(jid) for jid in prev_result.job_ids)

        read_started = time.perf_counter()
        rows = execute_read_query(
            team_id=team_id,
            job_ids=job_ids,
            current_start_utc=current_start_utc,
            current_end_utc=current_end_utc,
            previous_start_utc=previous_start_utc,
            previous_end_utc=previous_end_utc,
        )
        read_duration_ms = int((time.perf_counter() - read_started) * 1000)
        total_duration_ms = int((time.perf_counter() - overall_started) * 1000)

        logger.info(
            "web_overview_lazy_precompute_completed",
            team_id=team_id,
            job_count=len(result.job_ids),
            rows_returned=len(rows) if rows else 0,
            ensure_duration_ms=ensure_duration_ms,
            read_duration_ms=read_duration_ms,
            total_duration_ms=total_duration_ms,
        )
        if not rows:
            return _empty_response_row()
        return list(rows[0])
    except Exception as exc:
        WEB_OVERVIEW_LAZY_FAILED.labels(error_type=type(exc).__name__).inc()
        logger.exception(
            "web_overview_lazy_precompute_failed",
            team_id=team_id,
            error_type=type(exc).__name__,
            total_duration_ms=int((time.perf_counter() - overall_started) * 1000),
        )
        return None
