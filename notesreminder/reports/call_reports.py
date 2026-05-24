import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import sqlite3


WESTU_HOURS = {
    "mon": (14 * 60, 21 * 60),
    "tue": (14 * 60, 21 * 60),
    "wed": (14 * 60, 21 * 60),
    "thu": (14 * 60, 21 * 60),
    "fri": (14 * 60, 19 * 60 + 30),
    "sat": (9 * 60 + 30, 17 * 60),
    "sun": None,
}

HEIGHTS_HOURS = {
    "mon": (14 * 60 + 45, 20 * 60 + 30),
    "tue": (14 * 60 + 45, 20 * 60 + 30),
    "wed": (14 * 60 + 45, 20 * 60 + 30),
    "thu": (14 * 60 + 45, 20 * 60 + 30),
    "fri": None,
    "sat": (10 * 60, 15 * 60),
    "sun": None,
}


def minutes_since_midnight(ts):
    return ts.hour * 60 + ts.minute


def during_business_hours(ts, school):
    day = ts.strftime("%a").lower()[:3]
    hours = WESTU_HOURS if school == "West U" else HEIGHTS_HOURS
    window = hours.get(day)
    if not window:
        return False
    start, end = window
    return start <= minutes_since_midnight(ts) <= end


def resolve_school(name):
    if not isinstance(name, str):
        return None
    lowered = name.lower()
    if "west u" in lowered:
        return "West U"
    if "heights" in lowered:
        return "Heights"
    return None


def load_tables(conn):
    calls = pd.read_sql_query(
        "SELECT call_id, external_number, date_started, direction, category, name, is_internal "
        "FROM dialpad_calls",
        conn,
    )
    voicemails = pd.read_sql_query(
        "SELECT call_id, external_number, date, transcription_text "
        "FROM dialpad_voicemails",
        conn,
    )
    matches = pd.read_sql_query(
        "SELECT call_id, client_id FROM call_client_matches",
        conn,
    )
    return calls, voicemails, matches


def build_conversion_set(calls, matches):
    matched_calls = matches.merge(calls, on="call_id", how="left")
    return set(matched_calls["external_number"].dropna().unique())


def find_callback_minutes(calls, external_number, after_ts):
    window_end = after_ts + timedelta(days=7)
    subset = calls[
        (calls["external_number"] == external_number)
        & (calls["direction"] == "outbound")
        & (calls["date_started"] > after_ts)
        & (calls["date_started"] <= window_end)
    ].sort_values("date_started")
    if subset.empty:
        return None, None
    callback_time = subset.iloc[0]["date_started"]
    delta = (callback_time - after_ts).total_seconds() / 60.0
    return callback_time, round(delta, 2)


def build_voicemail_callbacks(calls, voicemails, converted_numbers):
    voicemails = voicemails.copy()
    voicemails["date"] = pd.to_datetime(voicemails["date"], errors="coerce")
    voicemails["transcription_text"] = voicemails["transcription_text"].fillna("")
    voicemails = voicemails[
        voicemails["date"].notna()
        & (voicemails["transcription_text"].str.len() > 30)
    ]

    calls_lookup = calls.set_index("call_id", drop=False)
    rows = []
    for _, row in voicemails.iterrows():
        call_id = row["call_id"]
        try:
            call_row = calls_lookup.loc[call_id]
        except KeyError:
            continue
        if isinstance(call_row, pd.DataFrame):
            call_row = call_row.iloc[0]
        school = resolve_school(call_row["name"])
        if not school:
            continue
        voicemail_time = row["date"]
        callback_time, callback_minutes = find_callback_minutes(
            calls, row["external_number"], voicemail_time
        )
        rows.append(
            {
                "school": school,
                "voicemail_date": voicemail_time.isoformat(sep=" ", timespec="seconds"),
                "callback_date": callback_time.isoformat(sep=" ", timespec="seconds")
                if callback_time
                else None,
                "callback_minutes": callback_minutes,
                "during_business_hours": during_business_hours(voicemail_time, school),
                "external_number": row["external_number"],
                "converted_to_student": row["external_number"] in converted_numbers,
            }
        )
    return pd.DataFrame(rows)


def build_missed_calls_no_vm(calls, voicemails, converted_numbers):
    calls = calls.copy()
    calls["date_started"] = pd.to_datetime(calls["date_started"], errors="coerce")
    calls = calls[calls["date_started"].notna()]

    voicemail_call_ids = set(voicemails["call_id"].dropna().unique())
    missed = calls[
        (calls["category"] == "missed")
        & (calls["direction"] == "inbound")
        & (calls["is_internal"] == "false")
        & (~calls["call_id"].isin(voicemail_call_ids))
    ]

    rows = []
    for _, row in missed.iterrows():
        school = resolve_school(row["name"])
        if not school:
            continue
        missed_time = row["date_started"]
        callback_time, callback_minutes = find_callback_minutes(
            calls, row["external_number"], missed_time
        )
        rows.append(
            {
                "school": school,
                "missed_call_date": missed_time.isoformat(sep=" ", timespec="seconds"),
                "callback_date": callback_time.isoformat(sep=" ", timespec="seconds")
                if callback_time
                else None,
                "callback_minutes": callback_minutes,
                "during_business_hours": during_business_hours(missed_time, school),
                "external_number": row["external_number"],
                "converted_to_student": row["external_number"] in converted_numbers,
            }
        )
    return pd.DataFrame(rows)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate voicemail/missed-call CSV reports.")
    parser.add_argument(
        "--db",
        default="/Users/hughscott/Documents/Coding/NotesReminder/reminders.db",
        help="Path to SQLite database",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    try:
        calls, voicemails, matches = load_tables(conn)
    finally:
        conn.close()

    calls["date_started"] = pd.to_datetime(calls["date_started"], errors="coerce")
    converted_numbers = build_conversion_set(calls, matches)

    voicemail_df = build_voicemail_callbacks(calls, voicemails, converted_numbers)
    missed_df = build_missed_calls_no_vm(calls, voicemails, converted_numbers)

    voicemail_out = db_path.parent / "voicemail_callbacks.csv"
    missed_out = db_path.parent / "missed_calls_no_vm.csv"

    voicemail_df.to_csv(voicemail_out, index=False)
    missed_df.to_csv(missed_out, index=False)

    print(f"✅ Wrote {voicemail_out}")
    print(f"✅ Wrote {missed_out}")


if __name__ == "__main__":
    main()
