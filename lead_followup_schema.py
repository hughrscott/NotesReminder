import argparse
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_INITIAL_LOAD_START = "2025-01-01"


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_email(value):
    if not value:
        return None
    value = str(value).strip().lower()
    return value or None


def normalize_phone(value):
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    return digits[-10:] if len(digits) >= 10 else digits


def _execute_many(conn, statements):
    for statement in statements:
        conn.execute(statement)


def ensure_lead_followup_schema(conn):
    """Create the V1 lead follow-up tables, indexes, and curated views."""
    _execute_many(
        conn,
        [
            """
            CREATE TABLE IF NOT EXISTS source_import_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                extractor TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                window_start TEXT,
                window_end TEXT,
                rows_seen INTEGER DEFAULT 0,
                rows_inserted INTEGER DEFAULT 0,
                rows_updated INTEGER DEFAULT 0,
                error TEXT,
                metadata_json TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS hubspot_deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT,
                stage TEXT,
                pipeline TEXT,
                owner TEXT,
                school TEXT,
                create_date TEXT,
                last_activity_date TEXT,
                last_contacted TEXT,
                follow_up_needed TEXT,
                trial_date TEXT,
                trial_no_show TEXT,
                date_entered_scheduled_trial_stage TEXT,
                area_of_interest TEXT,
                instrument_type TEXT,
                lead_source TEXT,
                marketing_source TEXT,
                pike13_person_id TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS hubspot_contacts (
                contact_id TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                full_name TEXT,
                email TEXT,
                email_normalized TEXT,
                phone TEXT,
                phone_normalized TEXT,
                sms_opt_in TEXT,
                owner TEXT,
                school TEXT,
                school_lead_status TEXT,
                associated_deal_ids TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS hubspot_tasks (
                task_id TEXT PRIMARY KEY,
                deal_id TEXT,
                contact_id TEXT,
                owner TEXT,
                due_date TEXT,
                completed_at TEXT,
                status TEXT,
                title TEXT,
                task_type TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS hubspot_activities (
                activity_id TEXT PRIMARY KEY,
                deal_id TEXT,
                contact_id TEXT,
                activity_type TEXT,
                activity_time TEXT,
                owner TEXT,
                title TEXT,
                body TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dialpad_sms_threads (
                thread_id TEXT PRIMARY KEY,
                feed_id TEXT,
                phone TEXT,
                phone_normalized TEXT,
                contact_name TEXT,
                last_message_at TEXT,
                unread_count INTEGER DEFAULT 0,
                school TEXT,
                department TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dialpad_sms_messages (
                message_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                message_at TEXT,
                direction TEXT,
                sender TEXT,
                recipient TEXT,
                body TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(thread_id) REFERENCES dialpad_sms_threads(thread_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dialpad_voice_events (
                event_id TEXT PRIMARY KEY,
                source_view TEXT,
                event_type TEXT,
                call_id TEXT,
                phone TEXT,
                phone_normalized TEXT,
                contact_name TEXT,
                direction TEXT,
                event_at TEXT,
                school TEXT,
                department TEXT,
                outcome TEXT,
                voicemail_transcript TEXT,
                recording_url TEXT,
                transcript_summary TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dialpad_call_reviews (
                call_review_id TEXT PRIMARY KEY,
                call_id TEXT,
                voice_event_id TEXT,
                call_review_url TEXT NOT NULL,
                event_at TEXT,
                transcript_text TEXT,
                recap_text TEXT,
                action_items_json TEXT,
                speaker_turns_json TEXT,
                transcript_available INTEGER DEFAULT 0,
                recap_available INTEGER DEFAULT 0,
                action_items_available INTEGER DEFAULT 0,
                audio_available INTEGER DEFAULT 0,
                extraction_status TEXT NOT NULL,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(voice_event_id) REFERENCES dialpad_voice_events(event_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dialpad_target_searches (
                search_id TEXT PRIMARY KEY,
                run_id INTEGER,
                deal_id TEXT,
                contact_id TEXT,
                target_hash TEXT NOT NULL,
                target_type TEXT NOT NULL,
                school TEXT,
                searched_at TEXT NOT NULL,
                search_paths_json TEXT,
                outcome TEXT NOT NULL,
                found_sms_count INTEGER DEFAULT 0,
                found_voice_count INTEGER DEFAULT 0,
                found_call_review_count INTEGER DEFAULT 0,
                source_url_count INTEGER DEFAULT 0,
                first_event_at TEXT,
                latest_event_at TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES source_import_runs(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dialpad_route_discoveries (
                route_id TEXT PRIMARY KEY,
                run_id INTEGER,
                route_name TEXT NOT NULL,
                route_url TEXT NOT NULL,
                status TEXT NOT NULL,
                loaded_at TEXT NOT NULL,
                supports_daily_refresh INTEGER DEFAULT 0,
                supports_targeted_search INTEGER DEFAULT 0,
                supports_date_filter INTEGER DEFAULT 0,
                supports_school_filter INTEGER DEFAULT 0,
                supports_keyword_filter INTEGER DEFAULT 0,
                visible_row_count INTEGER DEFAULT 0,
                visible_link_count INTEGER DEFAULT 0,
                call_review_url_count INTEGER DEFAULT 0,
                transcript_link_visible INTEGER DEFAULT 0,
                recording_link_visible INTEGER DEFAULT 0,
                download_link_visible INTEGER DEFAULT 0,
                sms_signal_visible INTEGER DEFAULT 0,
                voice_signal_visible INTEGER DEFAULT 0,
                voicemail_signal_visible INTEGER DEFAULT 0,
                required_filter_state TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES source_import_runs(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_route_discoveries (
                route_id TEXT PRIMARY KEY,
                run_id INTEGER,
                source TEXT NOT NULL,
                route_name TEXT NOT NULL,
                route_url TEXT,
                status TEXT NOT NULL,
                loaded_at TEXT NOT NULL,
                visible_row_count INTEGER DEFAULT 0,
                visible_link_count INTEGER DEFAULT 0,
                source_timestamp_visible INTEGER DEFAULT 0,
                transcript_link_visible INTEGER DEFAULT 0,
                recording_link_visible INTEGER DEFAULT 0,
                expected_controls_json TEXT,
                blocker TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES source_import_runs(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pike13_people (
                person_id TEXT PRIMARY KEY,
                full_name TEXT,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                email_normalized TEXT,
                phone TEXT,
                phone_normalized TEXT,
                membership_state TEXT,
                school TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pike13_visits (
                visit_id TEXT PRIMARY KEY,
                person_id TEXT,
                event_id TEXT,
                service TEXT,
                starts_at TEXT,
                status TEXT,
                no_show_flag INTEGER DEFAULT 0,
                unpaid_flag INTEGER DEFAULT 0,
                waiver_flag INTEGER DEFAULT 0,
                school TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(person_id) REFERENCES pike13_people(person_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pike13_plans_passes (
                plan_pass_id TEXT PRIMARY KEY,
                person_id TEXT,
                name TEXT,
                status TEXT,
                starts_at TEXT,
                ends_at TEXT,
                school TEXT,
                source_url TEXT,
                raw_text TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(person_id) REFERENCES pike13_people(person_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS identity_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_system TEXT NOT NULL,
                source_table TEXT NOT NULL,
                source_id TEXT NOT NULL,
                target_system TEXT NOT NULL,
                target_table TEXT NOT NULL,
                target_id TEXT NOT NULL,
                match_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                evidence TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(source_system, source_table, source_id, target_system, target_table, target_id, match_type)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS communication_ai_insights (
                insight_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_table TEXT NOT NULL,
                source_id TEXT NOT NULL,
                model TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                sentiment TEXT,
                intent TEXT,
                outcome TEXT,
                urgency TEXT,
                topic TEXT,
                action_items TEXT,
                summary TEXT,
                confidence REAL,
                raw_response_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(source_table, source_id, model, prompt_version)
            )
            """,
        ],
    )
    _execute_many(
        conn,
        [
            "CREATE INDEX IF NOT EXISTS idx_hubspot_deals_school ON hubspot_deals(school)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_deals_stage ON hubspot_deals(stage)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_deals_create_date ON hubspot_deals(create_date)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_deals_last_contacted ON hubspot_deals(last_contacted)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_deals_pike13 ON hubspot_deals(pike13_person_id)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_email ON hubspot_contacts(email_normalized)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_phone ON hubspot_contacts(phone_normalized)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_due ON hubspot_tasks(due_date)",
            "CREATE INDEX IF NOT EXISTS idx_hubspot_activities_time ON hubspot_activities(activity_time)",
            "CREATE INDEX IF NOT EXISTS idx_sms_threads_phone ON dialpad_sms_threads(phone_normalized)",
            "CREATE INDEX IF NOT EXISTS idx_sms_messages_time ON dialpad_sms_messages(message_at)",
            "CREATE INDEX IF NOT EXISTS idx_sms_messages_thread_time ON dialpad_sms_messages(thread_id, message_at)",
            "CREATE INDEX IF NOT EXISTS idx_voice_events_phone_time ON dialpad_voice_events(phone_normalized, event_at)",
            "CREATE INDEX IF NOT EXISTS idx_voice_events_type ON dialpad_voice_events(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_call_reviews_event ON dialpad_call_reviews(voice_event_id, event_at)",
            "CREATE INDEX IF NOT EXISTS idx_call_reviews_url ON dialpad_call_reviews(call_review_url)",
            "CREATE INDEX IF NOT EXISTS idx_call_reviews_call_id ON dialpad_call_reviews(call_id)",
            "CREATE INDEX IF NOT EXISTS idx_call_reviews_status ON dialpad_call_reviews(extraction_status)",
            "CREATE INDEX IF NOT EXISTS idx_target_searches_run ON dialpad_target_searches(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_target_searches_deal ON dialpad_target_searches(deal_id)",
            "CREATE INDEX IF NOT EXISTS idx_target_searches_outcome ON dialpad_target_searches(outcome)",
            "CREATE INDEX IF NOT EXISTS idx_target_searches_hash ON dialpad_target_searches(target_hash)",
            "CREATE INDEX IF NOT EXISTS idx_route_discoveries_run ON dialpad_route_discoveries(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_route_discoveries_status ON dialpad_route_discoveries(status)",
            "CREATE INDEX IF NOT EXISTS idx_route_discoveries_route ON dialpad_route_discoveries(route_name)",
            "CREATE INDEX IF NOT EXISTS idx_source_route_discoveries_source_status ON source_route_discoveries(source, status)",
            "CREATE INDEX IF NOT EXISTS idx_source_route_discoveries_run ON source_route_discoveries(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_source_route_discoveries_route ON source_route_discoveries(source, route_name)",
            "CREATE INDEX IF NOT EXISTS idx_pike13_people_email ON pike13_people(email_normalized)",
            "CREATE INDEX IF NOT EXISTS idx_pike13_people_phone ON pike13_people(phone_normalized)",
            "CREATE INDEX IF NOT EXISTS idx_pike13_visits_person_time ON pike13_visits(person_id, starts_at)",
            "CREATE INDEX IF NOT EXISTS idx_pike13_visits_time ON pike13_visits(starts_at)",
            "CREATE INDEX IF NOT EXISTS idx_pike13_plans_person_dates ON pike13_plans_passes(person_id, starts_at, ends_at)",
            "CREATE INDEX IF NOT EXISTS idx_comm_ai_source ON communication_ai_insights(source_table, source_id)",
            "CREATE INDEX IF NOT EXISTS idx_source_import_runs_source_status ON source_import_runs(source, status, started_at)",
        ],
    )
    _create_views(conn)


def _create_views(conn):
    _execute_many(
        conn,
        [
            "DROP VIEW IF EXISTS vw_lead_timeline",
            "DROP VIEW IF EXISTS vw_unanswered_messages",
            "DROP VIEW IF EXISTS vw_unanswered_communications",
            "DROP VIEW IF EXISTS vw_unmatched_dialpad_inbound",
            "DROP VIEW IF EXISTS vw_dialpad_daily_intake",
            "DROP VIEW IF EXISTS vw_dialpad_communications",
            "DROP VIEW IF EXISTS vw_pike13_lesson_visits",
            "DROP VIEW IF EXISTS vw_stale_leads",
            "DROP VIEW IF EXISTS vw_no_show_followup",
            "DROP VIEW IF EXISTS vw_lead_conversion_path",
            """
            CREATE VIEW vw_pike13_lesson_visits AS
            SELECT
                lesson_id AS visit_id,
                pike13_lesson_id,
                school,
                lesson_date,
                lesson_time,
                lesson_type,
                students,
                location,
                COALESCE(note_completed, 0) AS note_completed,
                CASE WHEN COALESCE(note_completed, 0) = 1 THEN 0 ELSE 1 END AS note_missing,
                attendance_status,
                CASE WHEN LOWER(COALESCE(attendance_status, '')) LIKE '%no show%' THEN 1 ELSE 0 END AS no_show_flag,
                CASE WHEN LOWER(COALESCE(attendance_status, '')) LIKE '%cancel%' THEN 1 ELSE 0 END AS canceled_flag,
                CASE WHEN LOWER(COALESCE(lesson_type, '')) LIKE '%trial%' THEN 1 ELSE 0 END AS trial_lesson_flag,
                CASE WHEN COALESCE(notes_text, '') != '' THEN 1 ELSE 0 END AS has_note_text,
                note_timestamp,
                CASE WHEN note_score IS NOT NULL THEN 1 ELSE 0 END AS has_note_score,
                last_checked AS updated_at
            FROM reminders
            WHERE lesson_id IS NOT NULL
              AND lesson_id != ''
            """,
            """
            CREATE VIEW vw_dialpad_communications AS
            SELECT
                'dialpad_sms_messages' AS source_table,
                m.message_id AS communication_id,
                'sms' AS channel,
                'sms' AS event_type,
                LOWER(COALESCE(m.direction, 'unknown')) AS direction,
                m.message_at AS event_at,
                t.phone,
                t.phone_normalized,
                t.contact_name,
                t.school,
                t.department,
                m.body,
                NULL AS summary,
                NULL AS outcome,
                COALESCE(m.source_url, t.source_url) AS source_url,
                0 AS has_transcript,
                CASE WHEN LOWER(COALESCE(m.direction, '')) = 'inbound' THEN 1 ELSE 0 END AS is_inbound_needing_followup,
                COALESCE(t.phone_normalized, m.thread_id) AS followup_key
            FROM dialpad_sms_messages m
            JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id
            UNION ALL
            SELECT
                'call_logs',
                c.call_id,
                'call',
                CASE
                    WHEN c.voicemail_transcript IS NOT NULL AND c.voicemail_transcript != '' THEN 'voicemail'
                    WHEN LOWER(COALESCE(c.category, '')) LIKE '%miss%' THEN 'missed_call'
                    ELSE 'call'
                END,
                LOWER(COALESCE(c.direction, 'unknown')),
                c.date_started,
                c.external_number,
                CASE
                    WHEN c.external_number IS NULL OR c.external_number = '' THEN NULL
                    ELSE substr(
                        replace(replace(replace(replace(replace(c.external_number, '+', ''), ' ', ''), '-', ''), '(', ''), ')', ''),
                        -10
                    )
                END,
                c.name,
                c.school_name,
                c.school_code,
                c.voicemail_transcript,
                rt.summary,
                COALESCE(rt.outcome, c.category),
                COALESCE(c.recording_url, c.voicemail_recording_url, rt.recording_url),
                CASE
                    WHEN COALESCE(c.voicemail_transcript, rt.transcript_text, rt.summary) IS NOT NULL THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN LOWER(COALESCE(c.direction, '')) LIKE '%inbound%'
                     AND (
                        LOWER(COALESCE(c.category, '')) LIKE '%miss%'
                        OR LOWER(COALESCE(c.category, '')) LIKE '%voicemail%'
                        OR c.voicemail_transcript IS NOT NULL
                     ) THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN c.external_number IS NULL OR c.external_number = '' THEN c.call_id
                    ELSE substr(
                        replace(replace(replace(replace(replace(c.external_number, '+', ''), ' ', ''), '-', ''), '(', ''), ')', ''),
                        -10
                    )
                END
            FROM call_logs c
            LEFT JOIN recording_transcripts rt ON rt.call_id = c.call_id
            UNION ALL
            SELECT
                'dialpad_voice_events',
                event_id,
                'call',
                event_type,
                LOWER(COALESCE(direction, 'unknown')),
                event_at,
                phone,
                phone_normalized,
                contact_name,
                school,
                department,
                voicemail_transcript,
                transcript_summary,
                outcome,
                COALESCE(source_url, recording_url),
                CASE WHEN COALESCE(voicemail_transcript, transcript_summary) IS NOT NULL THEN 1 ELSE 0 END,
                CASE
                    WHEN LOWER(COALESCE(direction, '')) LIKE '%inbound%'
                     AND LOWER(COALESCE(event_type, '')) IN ('missed_call', 'voicemail') THEN 1
                    ELSE 0
                END,
                COALESCE(phone_normalized, event_id)
            FROM dialpad_voice_events
            """,
            """
            CREATE VIEW vw_dialpad_daily_intake AS
            SELECT
                c.source_table,
                c.communication_id,
                c.channel,
                c.event_type,
                c.direction,
                c.event_at,
                c.phone_normalized,
                c.school,
                c.department,
                c.source_url,
                c.has_transcript,
                c.is_inbound_needing_followup,
                c.followup_key,
                (
                    SELECT COUNT(*)
                    FROM hubspot_contacts hc
                    WHERE COALESCE(hc.phone_normalized, '') != ''
                      AND hc.phone_normalized = c.phone_normalized
                      AND COALESCE(
                            json_extract(
                                CASE WHEN json_valid(COALESCE(hc.raw_json, '{}')) THEN hc.raw_json ELSE '{}' END,
                                '$.trusted'
                            ),
                            0
                          ) = 1
                      AND LOWER(COALESCE(hc.email_normalized, '')) NOT LIKE '%@schoolofrock.com'
                ) AS hubspot_contact_match_count,
                (
                    SELECT COUNT(*)
                    FROM pike13_people pp
                    WHERE COALESCE(pp.phone_normalized, '') != ''
                      AND pp.phone_normalized = c.phone_normalized
                ) AS pike13_person_match_count,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM vw_dialpad_communications later
                        WHERE later.followup_key = c.followup_key
                          AND later.event_at > c.event_at
                          AND LOWER(COALESCE(later.direction, '')) LIKE '%outbound%'
                    ) THEN 1
                    ELSE 0
                END AS has_later_outbound_followup,
                CASE
                    WHEN (
                        SELECT COUNT(*)
                        FROM hubspot_contacts hc
                        WHERE COALESCE(hc.phone_normalized, '') != ''
                          AND hc.phone_normalized = c.phone_normalized
                          AND COALESCE(
                                json_extract(
                                    CASE WHEN json_valid(COALESCE(hc.raw_json, '{}')) THEN hc.raw_json ELSE '{}' END,
                                    '$.trusted'
                                ),
                                0
                              ) = 1
                          AND LOWER(COALESCE(hc.email_normalized, '')) NOT LIKE '%@schoolofrock.com'
                    ) > 0 THEN 'matched_hubspot'
                    WHEN (
                        SELECT COUNT(*)
                        FROM pike13_people pp
                        WHERE COALESCE(pp.phone_normalized, '') != ''
                          AND pp.phone_normalized = c.phone_normalized
                    ) > 0 THEN 'matched_pike13_only'
                    ELSE 'unmatched'
                END AS match_status,
                CASE
                    WHEN c.is_inbound_needing_followup = 1
                     AND EXISTS (
                        SELECT 1
                        FROM vw_dialpad_communications later
                        WHERE later.followup_key = c.followup_key
                          AND later.event_at > c.event_at
                          AND LOWER(COALESCE(later.direction, '')) LIKE '%outbound%'
                     ) THEN 'followed_up'
                    WHEN c.is_inbound_needing_followup = 1
                     AND (
                        SELECT COUNT(*)
                        FROM hubspot_contacts hc
                        WHERE COALESCE(hc.phone_normalized, '') != ''
                          AND hc.phone_normalized = c.phone_normalized
                          AND COALESCE(
                                json_extract(
                                    CASE WHEN json_valid(COALESCE(hc.raw_json, '{}')) THEN hc.raw_json ELSE '{}' END,
                                    '$.trusted'
                                ),
                                0
                              ) = 1
                          AND LOWER(COALESCE(hc.email_normalized, '')) NOT LIKE '%@schoolofrock.com'
                     ) > 0 THEN 'matched_lead_review'
                    WHEN c.is_inbound_needing_followup = 1
                     AND (
                        SELECT COUNT(*)
                        FROM pike13_people pp
                        WHERE COALESCE(pp.phone_normalized, '') != ''
                          AND pp.phone_normalized = c.phone_normalized
                     ) > 0 THEN 'matched_pike13_review'
                    WHEN c.is_inbound_needing_followup = 1 THEN 'possible_lead_not_in_hubspot'
                    ELSE 'informational'
                END AS action_status
            FROM vw_dialpad_communications c
            WHERE date(c.event_at) IS NOT NULL
              AND date(c.event_at) <= date('now')
            """,
            """
            CREATE VIEW vw_unmatched_dialpad_inbound AS
            SELECT
                source_table,
                communication_id,
                channel,
                event_type,
                direction,
                event_at,
                phone_normalized,
                school,
                department,
                source_url,
                has_transcript,
                is_inbound_needing_followup,
                has_later_outbound_followup,
                match_status,
                action_status
            FROM vw_dialpad_daily_intake
            WHERE is_inbound_needing_followup = 1
              AND match_status != 'matched_hubspot'
            """,
            """
            CREATE VIEW vw_lead_timeline AS
            SELECT
                'hubspot' AS source,
                'deal' AS event_type,
                deal_id AS source_id,
                deal_id,
                NULL AS contact_id,
                pike13_person_id,
                create_date AS event_at,
                school,
                owner,
                deal_name AS person_or_lead,
                stage AS title,
                'Deal created/current stage: ' || COALESCE(stage, '') AS detail,
                source_url
            FROM hubspot_deals
            UNION ALL
            SELECT
                'hubspot',
                'task',
                task_id,
                deal_id,
                contact_id,
                NULL,
                COALESCE(completed_at, due_date),
                NULL,
                owner,
                NULL,
                title,
                COALESCE(status, '') || ' ' || COALESCE(task_type, ''),
                source_url
            FROM hubspot_tasks
            UNION ALL
            SELECT
                'hubspot',
                activity_type,
                activity_id,
                deal_id,
                contact_id,
                NULL,
                activity_time,
                NULL,
                owner,
                NULL,
                title,
                body,
                source_url
            FROM hubspot_activities
            UNION ALL
            SELECT
                'dialpad',
                channel || ':' || event_type,
                communication_id,
                NULL,
                NULL,
                NULL,
                event_at,
                school,
                department,
                COALESCE(contact_name, phone),
                COALESCE(outcome, direction, event_type),
                COALESCE(summary, body, ''),
                source_url
            FROM vw_dialpad_communications
            UNION ALL
            SELECT
                'pike13',
                'visit',
                v.visit_id,
                NULL,
                NULL,
                v.person_id,
                v.starts_at,
                COALESCE(v.school, p.school),
                NULL,
                p.full_name,
                v.service,
                COALESCE(v.status, '') ||
                    CASE WHEN v.no_show_flag = 1 THEN ' no-show' ELSE '' END ||
                    CASE WHEN v.unpaid_flag = 1 THEN ' unpaid' ELSE '' END,
                COALESCE(v.source_url, p.source_url)
            FROM pike13_visits v
            LEFT JOIN pike13_people p ON p.person_id = v.person_id
            """,
            "DROP VIEW IF EXISTS vw_stale_leads",
            """
            CREATE VIEW vw_stale_leads AS
            SELECT
                d.deal_id,
                d.deal_name,
                d.stage,
                d.owner,
                d.school,
                d.create_date,
                d.last_contacted,
                d.last_activity_date,
                d.follow_up_needed,
                d.trial_date,
                d.trial_no_show,
                d.pike13_person_id,
                CAST(julianday('now') - julianday(COALESCE(d.last_contacted, d.last_activity_date, d.create_date)) AS INTEGER) AS days_since_last_touch,
                CASE
                    WHEN LOWER(COALESCE(d.follow_up_needed, '')) IN ('yes', 'true', '1', 'follow up needed') THEN 'follow_up_needed'
                    WHEN EXISTS (
                        SELECT 1 FROM hubspot_tasks t
                        WHERE t.deal_id = d.deal_id
                          AND COALESCE(LOWER(t.status), '') NOT IN ('completed', 'done')
                          AND t.due_date IS NOT NULL
                          AND date(t.due_date) < date('now')
                    ) THEN 'overdue_task'
                    WHEN COALESCE(d.last_contacted, d.last_activity_date, d.create_date) IS NULL THEN 'missing_touch_date'
                    ELSE 'stale_touch'
                END AS risk_reason,
                d.source_url
            FROM hubspot_deals d
            WHERE LOWER(COALESCE(d.stage, '')) NOT LIKE '%closed%'
              AND LOWER(COALESCE(d.stage, '')) NOT LIKE '%lost%'
              AND LOWER(COALESCE(d.stage, '')) NOT LIKE '%enrolled%'
            """,
            "DROP VIEW IF EXISTS vw_unanswered_communications",
            """
            CREATE VIEW vw_unanswered_communications AS
            SELECT
                c.communication_id,
                c.source_table,
                c.channel,
                c.event_type,
                c.direction,
                c.event_at,
                c.phone,
                c.phone_normalized,
                c.contact_name,
                c.school,
                c.department,
                c.body,
                c.summary,
                c.outcome,
                CAST(julianday('now') - julianday(c.event_at) AS INTEGER) AS days_since_inbound,
                c.source_url
            FROM vw_dialpad_communications c
            WHERE c.is_inbound_needing_followup = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM vw_dialpad_communications later
                  WHERE later.followup_key = c.followup_key
                    AND later.event_at > c.event_at
                    AND LOWER(COALESCE(later.direction, '')) LIKE '%outbound%'
              )
            """,
            """
            CREATE VIEW vw_unanswered_messages AS
            SELECT
                communication_id AS message_id,
                communication_id AS thread_id,
                contact_name,
                phone,
                phone_normalized,
                school,
                department,
                event_at AS message_at,
                body,
                days_since_inbound,
                source_url
            FROM vw_unanswered_communications
            WHERE channel = 'sms'
            """,
            "DROP VIEW IF EXISTS vw_no_show_followup",
            """
            CREATE VIEW vw_no_show_followup AS
            SELECT
                v.visit_id,
                v.person_id,
                p.full_name,
                COALESCE(v.school, p.school, d.school) AS school,
                v.service,
                v.starts_at,
                v.status,
                d.deal_id,
                d.deal_name,
                d.stage,
                d.owner,
                d.last_contacted,
                d.follow_up_needed,
                CAST(julianday('now') - julianday(v.starts_at) AS INTEGER) AS days_since_no_show,
                COALESCE(v.source_url, p.source_url, d.source_url) AS source_url
            FROM pike13_visits v
            LEFT JOIN pike13_people p ON p.person_id = v.person_id
            LEFT JOIN hubspot_deals d ON d.pike13_person_id = v.person_id
            WHERE v.no_show_flag = 1 OR LOWER(COALESCE(v.status, '')) LIKE '%no%show%'
            """,
            "DROP VIEW IF EXISTS vw_lead_conversion_path",
            """
            CREATE VIEW vw_lead_conversion_path AS
            SELECT
                d.deal_id,
                d.deal_name,
                d.school,
                d.owner,
                d.create_date AS lead_created_at,
                d.last_contacted,
                d.stage,
                d.trial_date,
                d.trial_no_show,
                d.pike13_person_id,
                MIN(v.starts_at) AS first_visit_at,
                GROUP_CONCAT(DISTINCT v.status) AS visit_statuses,
                GROUP_CONCAT(DISTINCT pp.name) AS plans_or_passes,
                GROUP_CONCAT(DISTINCT pp.status) AS plan_pass_statuses,
                CASE
                    WHEN MAX(CASE WHEN LOWER(COALESCE(pp.status, '')) IN ('active', 'upcoming') THEN 1 ELSE 0 END) = 1 THEN 'enrolled_or_active'
                    WHEN MAX(CASE WHEN v.no_show_flag = 1 OR LOWER(COALESCE(v.status, '')) LIKE '%no%show%' THEN 1 ELSE 0 END) = 1 THEN 'trial_no_show'
                    WHEN MIN(v.starts_at) IS NOT NULL THEN 'trial_attended_or_scheduled'
                    WHEN d.trial_date IS NOT NULL THEN 'trial_booked'
                    WHEN d.last_contacted IS NOT NULL THEN 'contacted'
                    ELSE 'created'
                END AS conversion_state,
                d.source_url
            FROM hubspot_deals d
            LEFT JOIN pike13_visits v ON v.person_id = d.pike13_person_id
            LEFT JOIN pike13_plans_passes pp ON pp.person_id = d.pike13_person_id
            GROUP BY d.deal_id
            """,
        ],
    )


def start_import_run(conn, source, extractor, window_start=None, window_end=None, metadata=None):
    started_at = utc_now_iso()
    cursor = conn.execute(
        """
        INSERT INTO source_import_runs
        (source, extractor, started_at, status, window_start, window_end, metadata_json)
        VALUES (?, ?, ?, 'running', ?, ?, ?)
        """,
        (
            source,
            extractor,
            started_at,
            window_start,
            window_end,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    return cursor.lastrowid


def finish_import_run(
    conn,
    run_id,
    status,
    rows_seen=0,
    rows_inserted=0,
    rows_updated=0,
    error=None,
    metadata=None,
):
    conn.execute(
        """
        UPDATE source_import_runs
        SET finished_at = ?,
            status = ?,
            rows_seen = ?,
            rows_inserted = ?,
            rows_updated = ?,
            error = ?,
            metadata_json = COALESCE(?, metadata_json)
        WHERE id = ?
        """,
        (
            utc_now_iso(),
            status,
            rows_seen,
            rows_inserted,
            rows_updated,
            error,
            json.dumps(metadata, sort_keys=True) if metadata is not None else None,
            run_id,
        ),
    )


def upsert_identity_match(
    conn,
    source_system,
    source_table,
    source_id,
    target_system,
    target_table,
    target_id,
    match_type,
    confidence,
    evidence=None,
):
    conn.execute(
        """
        INSERT INTO identity_matches
        (source_system, source_table, source_id, target_system, target_table, target_id,
         match_type, confidence, evidence, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_system, source_table, source_id, target_system, target_table, target_id, match_type)
        DO UPDATE SET confidence = excluded.confidence, evidence = excluded.evidence
        """,
        (
            source_system,
            source_table,
            str(source_id),
            target_system,
            target_table,
            str(target_id),
            match_type,
            confidence,
            evidence,
            utc_now_iso(),
        ),
    )


def main():
    parser = argparse.ArgumentParser(description="Create V1 lead follow-up schema.")
    parser.add_argument("--db", default="reminders.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    try:
        ensure_lead_followup_schema(conn)
        conn.commit()
    finally:
        conn.close()
    print(f"Lead follow-up schema is ready in {db_path}")


if __name__ == "__main__":
    main()
