"""Experimental, auditable communication insight generation."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from lead_followup_schema import ensure_lead_followup_schema


PROMPT_VERSION = "phase19-communication-insights-v1"
DEFAULT_MODEL = "heuristic-reviewer-v1"
MAX_TEXT_CHARS = 6000
SUPPORTED_SOURCE_TABLES = (
    "dialpad_call_reviews",
    "dialpad_voice_events",
    "dialpad_sms_messages",
    "school_email_messages",
)


@dataclass(frozen=True)
class SourceEvent:
    source_table: str
    source_id: str
    event_at: str | None
    school: str | None
    source_url: str | None
    source_text: str
    evidence_label: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def compact_text(value: str | None, limit: int = MAX_TEXT_CHARS) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table,),
        ).fetchone()
    )


def build_prompt(event: SourceEvent) -> str:
    return (
        "Analyze the customer communication for sentiment, intent, urgency, outcome, "
        "topic, action items, and a concise operator recommendation. "
        "Use only the provided source text. Keep output factual and auditable. "
        f"Source table: {event.source_table}. Source ID: {event.source_id}. "
        f"Text: {compact_text(event.source_text)}"
    )


def _append_events(rows: list[SourceEvent], seen: set[tuple[str, str]], candidates: list[SourceEvent], limit: int) -> None:
    for event in candidates:
        if len(rows) >= limit:
            return
        key = (event.source_table, event.source_id)
        if key in seen or not compact_text(event.source_text):
            continue
        seen.add(key)
        rows.append(event)


def collect_source_events(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    school: str | None = None,
    limit: int = 50,
) -> list[SourceEvent]:
    params = {"start_date": start_date, "end_date": end_date, "school": school or ""}
    events: list[SourceEvent] = []
    seen: set[tuple[str, str]] = set()

    if table_exists(conn, "dialpad_call_reviews"):
        rows = conn.execute(
            """
            SELECT
                cr.call_review_id AS source_id,
                cr.event_at,
                COALESCE(v.school, '') AS school,
                cr.call_review_url AS source_url,
                TRIM(COALESCE(cr.recap_text, '') || ' ' || COALESCE(cr.transcript_text, '')) AS source_text
            FROM dialpad_call_reviews cr
            LEFT JOIN dialpad_voice_events v ON v.event_id = cr.voice_event_id
            WHERE date(COALESCE(cr.event_at, cr.updated_at)) BETWEEN date(:start_date) AND date(:end_date)
              AND (:school = '' OR LOWER(COALESCE(v.school, '')) LIKE '%' || LOWER(:school) || '%')
              AND TRIM(COALESCE(cr.recap_text, '') || COALESCE(cr.transcript_text, '')) <> ''
            ORDER BY COALESCE(cr.event_at, cr.updated_at) DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        _append_events(
            events,
            seen,
            [
                SourceEvent(
                    "dialpad_call_reviews",
                    row["source_id"],
                    row["event_at"],
                    row["school"],
                    row["source_url"],
                    row["source_text"],
                    "Dialpad call review",
                )
                for row in rows
            ],
            limit,
        )

    if len(events) < limit and table_exists(conn, "school_email_messages"):
        rows = conn.execute(
            """
            SELECT
                message_id AS source_id,
                message_at AS event_at,
                school,
                source_url,
                TRIM(COALESCE(subject, '') || ' ' || COALESCE(snippet, '') || ' ' || COALESCE(body, '')) AS source_text
            FROM school_email_messages
            WHERE date(COALESCE(message_at, updated_at)) BETWEEN date(:start_date) AND date(:end_date)
              AND (:school = '' OR LOWER(COALESCE(school, '')) LIKE '%' || LOWER(:school) || '%')
              AND TRIM(COALESCE(subject, '') || COALESCE(snippet, '') || COALESCE(body, '')) <> ''
            ORDER BY COALESCE(message_at, updated_at) DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        _append_events(
            events,
            seen,
            [
                SourceEvent(
                    "school_email_messages",
                    row["source_id"],
                    row["event_at"],
                    row["school"],
                    row["source_url"],
                    row["source_text"],
                    "School email",
                )
                for row in rows
            ],
            limit,
        )

    if len(events) < limit and table_exists(conn, "dialpad_sms_messages"):
        rows = conn.execute(
            """
            SELECT
                m.message_id AS source_id,
                m.message_at AS event_at,
                t.school,
                m.source_url,
                m.body AS source_text
            FROM dialpad_sms_messages m
            LEFT JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id
            WHERE date(COALESCE(m.message_at, m.updated_at)) BETWEEN date(:start_date) AND date(:end_date)
              AND (:school = '' OR LOWER(COALESCE(t.school, '')) LIKE '%' || LOWER(:school) || '%')
              AND TRIM(COALESCE(m.body, '')) <> ''
            ORDER BY COALESCE(m.message_at, m.updated_at) DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        _append_events(
            events,
            seen,
            [
                SourceEvent(
                    "dialpad_sms_messages",
                    row["source_id"],
                    row["event_at"],
                    row["school"],
                    row["source_url"],
                    row["source_text"],
                    "Dialpad SMS",
                )
                for row in rows
            ],
            limit,
        )

    if len(events) < limit and table_exists(conn, "dialpad_voice_events"):
        rows = conn.execute(
            """
            SELECT
                event_id AS source_id,
                event_at,
                school,
                source_url,
                TRIM(COALESCE(transcript_summary, '') || ' ' || COALESCE(voicemail_transcript, '') || ' ' || COALESCE(raw_text, '')) AS source_text
            FROM dialpad_voice_events
            WHERE date(COALESCE(event_at, updated_at)) BETWEEN date(:start_date) AND date(:end_date)
              AND (:school = '' OR LOWER(COALESCE(school, '')) LIKE '%' || LOWER(:school) || '%')
              AND TRIM(COALESCE(transcript_summary, '') || COALESCE(voicemail_transcript, '') || COALESCE(raw_text, '')) <> ''
            ORDER BY COALESCE(event_at, updated_at) DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        _append_events(
            events,
            seen,
            [
                SourceEvent(
                    "dialpad_voice_events",
                    row["source_id"],
                    row["event_at"],
                    row["school"],
                    row["source_url"],
                    row["source_text"],
                    "Dialpad voice",
                )
                for row in rows
            ],
            limit,
        )

    return events[:limit]


def heuristic_insight(event: SourceEvent) -> dict:
    text = compact_text(event.source_text).lower()
    signals = {
        "callback": any(term in text for term in ("call me", "call back", "callback", "voicemail")),
        "trial": any(term in text for term in ("trial", "tour", "first lesson", "camp")),
        "reschedule": any(term in text for term in ("reschedule", "move the lesson", "another time")),
        "cancel": any(term in text for term in ("cancel", "cancellation", "quit", "refund")),
        "billing": any(term in text for term in ("billing", "invoice", "payment", "charge", "refund")),
        "concern": any(term in text for term in ("concern", "worried", "problem", "upset", "frustrated")),
        "positive": any(term in text for term in ("thank", "thanks", "excited", "great", "interested")),
    }
    if signals["cancel"] or signals["concern"]:
        sentiment = "negative"
    elif signals["positive"]:
        sentiment = "positive"
    else:
        sentiment = "neutral"
    if signals["cancel"]:
        intent = "cancellation_or_retention_risk"
        recommendation = "Review promptly and confirm the retention or cancellation next step."
    elif signals["billing"]:
        intent = "billing_question"
        recommendation = "Route to the operator responsible for billing follow-up."
    elif signals["reschedule"]:
        intent = "reschedule_request"
        recommendation = "Confirm the requested scheduling change and document the outcome."
    elif signals["trial"]:
        intent = "trial_or_tour_interest"
        recommendation = "Verify trial/tour booking status and close the loop if no next step is logged."
    elif signals["callback"]:
        intent = "callback_request"
        recommendation = "Confirm outbound follow-up happened after the inbound request."
    else:
        intent = "general_followup"
        recommendation = "Review for any missing owner action before closing the loop."
    urgency = "high" if signals["cancel"] or "urgent" in text or "asap" in text else "medium" if any(signals.values()) else "low"
    outcome = "follow_up_recommended" if urgency in {"high", "medium"} else "monitor"
    action_items = "Human review required before this insight drives staff or customer action."
    topic = "retention" if signals["cancel"] else "billing" if signals["billing"] else "trial" if signals["trial"] else "operations"
    summary = f"{event.evidence_label} classified as {intent} with {urgency} urgency."
    confidence = 0.75 if any(signals.values()) else 0.45
    return {
        "sentiment": sentiment,
        "intent": intent,
        "outcome": outcome,
        "urgency": urgency,
        "topic": topic,
        "action_items": action_items,
        "summary": summary,
        "recommendation": recommendation,
        "confidence": confidence,
        "signals": signals,
    }


def build_evidence(event: SourceEvent) -> dict:
    return {
        "source_table": event.source_table,
        "source_id": event.source_id,
        "event_at": event.event_at,
        "school": event.school,
        "source_url": event.source_url,
        "source_text_sha256": text_sha256(compact_text(event.source_text)),
        "prompt_version": PROMPT_VERSION,
    }


def upsert_insight(
    conn: sqlite3.Connection,
    event: SourceEvent,
    insight: dict,
    *,
    model: str = DEFAULT_MODEL,
    prompt_version: str = PROMPT_VERSION,
    insight_run_id: str,
) -> None:
    ensure_lead_followup_schema(conn)
    evidence = build_evidence(event)
    raw_response = {
        "provider": "deterministic_heuristic",
        "prompt_hash": text_sha256(build_prompt(event)),
        "source_text_sha256": evidence["source_text_sha256"],
        "signals": insight.get("signals", {}),
    }
    conn.execute(
        """
        INSERT INTO communication_ai_insights (
            source_table, source_id, insight_run_id, model, prompt_version,
            sentiment, intent, outcome, urgency, topic, action_items, summary,
            recommendation, confidence, evidence_json, review_status,
            raw_response_json, created_at
        )
        VALUES (
            :source_table, :source_id, :insight_run_id, :model, :prompt_version,
            :sentiment, :intent, :outcome, :urgency, :topic, :action_items, :summary,
            :recommendation, :confidence, :evidence_json, 'pending_human_review',
            :raw_response_json, :created_at
        )
        ON CONFLICT(source_table, source_id, model, prompt_version) DO UPDATE SET
            insight_run_id = excluded.insight_run_id,
            sentiment = excluded.sentiment,
            intent = excluded.intent,
            outcome = excluded.outcome,
            urgency = excluded.urgency,
            topic = excluded.topic,
            action_items = excluded.action_items,
            summary = excluded.summary,
            recommendation = excluded.recommendation,
            confidence = excluded.confidence,
            evidence_json = excluded.evidence_json,
            review_status = excluded.review_status,
            raw_response_json = excluded.raw_response_json,
            created_at = excluded.created_at
        """,
        {
            "source_table": event.source_table,
            "source_id": event.source_id,
            "insight_run_id": insight_run_id,
            "model": model,
            "prompt_version": prompt_version,
            "sentiment": insight["sentiment"],
            "intent": insight["intent"],
            "outcome": insight["outcome"],
            "urgency": insight["urgency"],
            "topic": insight["topic"],
            "action_items": insight["action_items"],
            "summary": insight["summary"],
            "recommendation": insight["recommendation"],
            "confidence": insight["confidence"],
            "evidence_json": json.dumps(evidence, sort_keys=True),
            "raw_response_json": json.dumps(raw_response, sort_keys=True),
            "created_at": utc_now_iso(),
        },
    )


def generate_insights(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    school: str | None = None,
    limit: int = 25,
    model: str = DEFAULT_MODEL,
    prompt_version: str = PROMPT_VERSION,
    dry_run: bool = False,
) -> dict:
    ensure_lead_followup_schema(conn)
    events = collect_source_events(conn, start_date, end_date, school, limit)
    insight_run_id = f"insight_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    rows = []
    for event in events:
        insight = heuristic_insight(event)
        evidence = build_evidence(event)
        row = {
            "source_table": event.source_table,
            "source_id": event.source_id,
            "event_at": event.event_at,
            "school": event.school,
            "sentiment": insight["sentiment"],
            "intent": insight["intent"],
            "outcome": insight["outcome"],
            "urgency": insight["urgency"],
            "topic": insight["topic"],
            "summary": insight["summary"],
            "recommendation": insight["recommendation"],
            "confidence": insight["confidence"],
            "review_status": "pending_human_review",
            "evidence": evidence,
        }
        rows.append(row)
        if not dry_run:
            upsert_insight(
                conn,
                event,
                insight,
                model=model,
                prompt_version=prompt_version,
                insight_run_id=insight_run_id,
            )
    return {
        "status": "ready",
        "mode": "experimental",
        "insight_run_id": insight_run_id,
        "window": {"start_date": start_date, "end_date": end_date},
        "filters": {"school": school or ""},
        "model": model,
        "prompt_version": prompt_version,
        "dry_run": dry_run,
        "rows_seen": len(events),
        "rows_written": 0 if dry_run else len(events),
        "insights": rows,
        "sensitive_content_included": False,
    }


def render_review_markdown(report: dict) -> str:
    lines = [
        "# Experimental Communication Insights",
        "",
        f"Window: {report['window']['start_date']} to {report['window']['end_date']}",
        f"School filter: {report['filters'].get('school') or 'All schools'}",
        f"Run ID: {report['insight_run_id']}",
        f"Mode: {report['mode']}",
        f"Dry run: {report['dry_run']}",
        "",
        "| Source | Event At | School | Sentiment | Intent | Urgency | Recommendation | Review |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in report["insights"]:
        source = f"{row['source_table']}:{row['source_id']}"
        lines.append(
            "| "
            + " | ".join(
                str(value or "").replace("|", "\\|")
                for value in (
                    source,
                    row["event_at"],
                    row["school"],
                    row["sentiment"],
                    row["intent"],
                    row["urgency"],
                    row["recommendation"],
                    row["review_status"],
                )
            )
            + " |"
        )
    if not report["insights"]:
        lines.append("| None | | | | | | | |")
    lines.extend(
        [
            "",
            "_This experimental review output is sanitized: it excludes customer names, phones, emails, message bodies, transcripts, raw notes, source URLs, and audio paths._",
        ]
    )
    return "\n".join(lines) + "\n"


def report_to_json(report: dict) -> str:
    return json.dumps(report, indent=2, default=str)
