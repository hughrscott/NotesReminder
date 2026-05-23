"""Deterministic person identity resolution across source systems."""

import hashlib
import json
import re
from collections import Counter, defaultdict

from lead_followup_schema import ensure_lead_followup_schema, normalize_email, normalize_phone, utc_now_iso


SOR_EMAIL_RE = re.compile(r"@schoolofrock\.com$", re.IGNORECASE)
GENERIC_NAMES = {"loading", "unknown", "anonymous", "contact", "none"}


class UnionFind:
    def __init__(self):
        self.parent = {}

    def add(self, item):
        self.parent.setdefault(item, item)

    def find(self, item):
        self.add(item)
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left, right):
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        self.parent[max(left_root, right_root)] = min(left_root, right_root)


def _school(value):
    return str(value or "").strip() or None


def _name(value):
    cleaned = str(value or "").strip()
    if not cleaned or cleaned.lower() in GENERIC_NAMES:
        return None
    return cleaned


def _email(value):
    normalized = normalize_email(value)
    if not normalized or SOR_EMAIL_RE.search(normalized):
        return None
    return normalized


def _phone(value):
    normalized = normalize_phone(value)
    if not normalized or len(normalized) < 7:
        return None
    return normalized


def _identity(identity_type, value):
    if not value:
        return None
    return f"{identity_type}:{value}"


def _source_node(source_table, source_id):
    return f"source:{source_table}:{source_id}"


def _person_id_for(identity_keys):
    priority = ("pike13_person", "hubspot_contact", "email", "phone", "school_email", "dialpad")
    by_type = defaultdict(list)
    for key in identity_keys:
        identity_type, value = key.split(":", 1)
        by_type[identity_type].append(value)
    for identity_type in priority:
        if by_type[identity_type]:
            raw = f"{identity_type}:{sorted(by_type[identity_type])[0]}"
            return "person_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    raw = sorted(identity_keys)[0]
    return "person_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _choose(values):
    cleaned = [value for value in values if value]
    if not cleaned:
        return None
    counts = Counter(cleaned)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _choose_display_name(records):
    source_priority = ("hubspot_contacts", "pike13_people", "hubspot_deals", "dialpad_sms_threads", "dialpad_voice_events")
    for source_table in source_priority:
        names = [record["name"] for record in records if record["source_table"] == source_table and record["name"]]
        if names:
            return _choose(names)
    return _choose([record["name"] for record in records])


def _parse_deal_ids(value):
    return [item for item in re.split(r"[,;\s]+", str(value or "")) if item]


def _source_records(conn):
    for row in conn.execute(
        """
        SELECT contact_id, full_name, email_normalized, phone_normalized,
               associated_deal_ids, school
        FROM hubspot_contacts
        """
    ).fetchall():
        identities = [_identity("hubspot_contact", row["contact_id"])]
        identities.append(_identity("email", _email(row["email_normalized"])))
        identities.append(_identity("phone", _phone(row["phone_normalized"])))
        for deal_id in _parse_deal_ids(row["associated_deal_ids"]):
            identities.append(_identity("hubspot_deal", deal_id))
        yield {
            "source_system": "hubspot",
            "source_table": "hubspot_contacts",
            "source_id": row["contact_id"],
            "name": _name(row["full_name"]),
            "email": _email(row["email_normalized"]),
            "phone": _phone(row["phone_normalized"]),
            "school": _school(row["school"]),
            "identities": [item for item in identities if item],
        }

    for row in conn.execute(
        """
        SELECT deal_id, deal_name, pike13_person_id, school
        FROM hubspot_deals
        """
    ).fetchall():
        identities = [_identity("hubspot_deal", row["deal_id"])]
        identities.append(_identity("pike13_person", row["pike13_person_id"]))
        yield {
            "source_system": "hubspot",
            "source_table": "hubspot_deals",
            "source_id": row["deal_id"],
            "name": _name(row["deal_name"]),
            "email": None,
            "phone": None,
            "school": _school(row["school"]),
            "identities": [item for item in identities if item],
        }

    for row in conn.execute(
        """
        SELECT person_id, full_name, email_normalized, phone_normalized, school
        FROM pike13_people
        """
    ).fetchall():
        identities = [_identity("pike13_person", row["person_id"])]
        identities.append(_identity("email", _email(row["email_normalized"])))
        identities.append(_identity("phone", _phone(row["phone_normalized"])))
        yield {
            "source_system": "pike13",
            "source_table": "pike13_people",
            "source_id": row["person_id"],
            "name": _name(row["full_name"]),
            "email": _email(row["email_normalized"]),
            "phone": _phone(row["phone_normalized"]),
            "school": _school(row["school"]),
            "identities": [item for item in identities if item],
        }

    for table, id_col, name_col in (
        ("dialpad_sms_threads", "thread_id", "contact_name"),
        ("dialpad_voice_events", "event_id", "contact_name"),
    ):
        for row in conn.execute(
            f"""
            SELECT {id_col} AS source_id, {name_col} AS name,
                   phone_normalized, school
            FROM {table}
            WHERE COALESCE(phone_normalized, '') != ''
            """
        ).fetchall():
            identities = [_identity("dialpad", f"{table}:{row['source_id']}")]
            identities.append(_identity("phone", _phone(row["phone_normalized"])))
            yield {
                "source_system": "dialpad",
                "source_table": table,
                "source_id": row["source_id"],
                "name": _name(row["name"]),
                "email": None,
                "phone": _phone(row["phone_normalized"]),
                "school": _school(row["school"]),
                "identities": [item for item in identities if item],
            }

    for row in conn.execute(
        """
        SELECT message_id, external_email_normalized, school
        FROM school_email_messages
        WHERE COALESCE(external_email_normalized, '') != ''
        """
    ).fetchall():
        email = _email(row["external_email_normalized"])
        identities = [_identity("school_email", row["message_id"]), _identity("email", email)]
        yield {
            "source_system": "school_email",
            "source_table": "school_email_messages",
            "source_id": row["message_id"],
            "name": None,
            "email": email,
            "phone": None,
            "school": _school(row["school"]),
            "identities": [item for item in identities if item],
        }


def _source_update_column(source_table):
    if source_table == "pike13_people":
        return "person_identity_id"
    return "person_id"


def refresh_person_identities(conn):
    """Rebuild deterministic person identities from exact source identifiers."""
    ensure_lead_followup_schema(conn)
    conn.execute("DELETE FROM person_resolution_conflicts")
    conn.execute("DELETE FROM person_identities")
    conn.execute("DELETE FROM persons")
    for table, column in (
        ("hubspot_deals", "person_id"),
        ("hubspot_contacts", "person_id"),
        ("pike13_people", "person_identity_id"),
        ("pike13_visits", "person_identity_id"),
        ("pike13_plans_passes", "person_identity_id"),
        ("dialpad_sms_threads", "person_id"),
        ("dialpad_voice_events", "person_id"),
        ("school_email_messages", "person_id"),
    ):
        conn.execute(f"UPDATE {table} SET {column} = NULL")

    records = list(_source_records(conn))
    uf = UnionFind()
    record_by_node = {}
    for record in records:
        source_node = _source_node(record["source_table"], record["source_id"])
        record_by_node[source_node] = record
        uf.add(source_node)
        for identity_key in record["identities"]:
            uf.union(source_node, identity_key)

    components = defaultdict(lambda: {"nodes": set(), "records": [], "identities": set()})
    for source_node, record in record_by_node.items():
        root = uf.find(source_node)
        components[root]["nodes"].add(source_node)
        components[root]["records"].append(record)
        components[root]["identities"].update(record["identities"])

    now = utc_now_iso()
    source_to_person = {}
    person_count = 0
    identity_count = 0
    conflict_count = 0
    for component in components.values():
        identities = sorted(component["identities"])
        if not identities:
            continue
        person_id = _person_id_for(identities)
        records_for_person = component["records"]
        display_name = _choose_display_name(records_for_person)
        primary_email = _choose([record["email"] for record in records_for_person])
        primary_phone = _choose([record["phone"] for record in records_for_person])
        school = _choose([record["school"] for record in records_for_person])
        by_type = defaultdict(list)
        for identity_key in identities:
            identity_type, identity_value = identity_key.split(":", 1)
            by_type[identity_type].append(identity_value)
        conflict_types = []
        for singleton_type in ("pike13_person", "hubspot_contact"):
            if len(set(by_type.get(singleton_type, []))) > 1:
                conflict_types.append(f"multiple_{singleton_type}")
        resolution_status = "conflict" if conflict_types else "resolved"
        conn.execute(
            """
            INSERT INTO persons (
                person_id, display_name, primary_email, primary_phone, school,
                resolution_status, source_count, identity_count, raw_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                person_id,
                display_name,
                primary_email,
                primary_phone,
                school,
                resolution_status,
                len(records_for_person),
                len(identities),
                json.dumps({"identity_keys": identities}, sort_keys=True),
                now,
                now,
            ),
        )
        person_count += 1
        for record in records_for_person:
            source_to_person[(record["source_table"], record["source_id"])] = person_id
            for identity_key in record["identities"]:
                identity_type, identity_value = identity_key.split(":", 1)
                confidence = 0.99 if identity_type in {"hubspot_contact", "hubspot_deal", "pike13_person"} else 0.95
                conn.execute(
                    """
                    INSERT OR IGNORE INTO person_identities (
                        person_id, identity_type, identity_value, source_system,
                        source_table, source_id, confidence, evidence, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        person_id,
                        identity_type,
                        identity_value,
                        record["source_system"],
                        record["source_table"],
                        str(record["source_id"]),
                        confidence,
                        "deterministic exact identity resolver",
                        now,
                    ),
                )
                identity_count += conn.total_changes
        for conflict_type in conflict_types:
            conn.execute(
                """
                INSERT INTO person_resolution_conflicts (
                    conflict_type, person_ids_json, evidence_json, status, created_at
                )
                VALUES (?, ?, ?, 'open', ?)
                """,
                (
                    conflict_type,
                    json.dumps([person_id]),
                    json.dumps({"identity_keys": identities}, sort_keys=True),
                    now,
                ),
            )
            conflict_count += 1

    for (source_table, source_id), person_id in source_to_person.items():
        column = _source_update_column(source_table)
        source_id_column = _source_id_column(source_table)
        conn.execute(
            f"UPDATE {source_table} SET {column} = ? WHERE {source_id_column} = ?",
            (person_id, source_id),
        )

    conn.execute(
        """
        UPDATE pike13_visits
        SET person_identity_id = (
            SELECT person_identity_id
            FROM pike13_people
            WHERE pike13_people.person_id = pike13_visits.person_id
        )
        WHERE person_id IS NOT NULL
        """
    )
    conn.execute(
        """
        UPDATE pike13_plans_passes
        SET person_identity_id = (
            SELECT person_identity_id
            FROM pike13_people
            WHERE pike13_people.person_id = pike13_plans_passes.person_id
        )
        WHERE person_id IS NOT NULL
        """
    )
    return {
        "persons": person_count,
        "person_identities": conn.execute("SELECT COUNT(*) FROM person_identities").fetchone()[0],
        "conflicts": conflict_count,
        "linked_sources": len(source_to_person),
    }


def _source_id_column(source_table):
    return {
        "hubspot_deals": "deal_id",
        "hubspot_contacts": "contact_id",
        "pike13_people": "person_id",
        "dialpad_sms_threads": "thread_id",
        "dialpad_voice_events": "event_id",
        "school_email_messages": "message_id",
    }[source_table]


def person_search(conn, query, limit=20):
    ensure_lead_followup_schema(conn)
    needle = f"%{str(query or '').strip().lower()}%"
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT person_id, display_name, primary_email, primary_phone, school,
                   resolution_status, source_count, identity_count
            FROM persons
            WHERE LOWER(COALESCE(person_id, '')) LIKE :needle
               OR LOWER(COALESCE(display_name, '')) LIKE :needle
               OR LOWER(COALESCE(primary_email, '')) LIKE :needle
               OR LOWER(COALESCE(primary_phone, '')) LIKE :needle
               OR LOWER(COALESCE(school, '')) LIKE :needle
            ORDER BY source_count DESC, display_name
            LIMIT :limit
            """,
            {"needle": needle, "limit": max(1, min(int(limit), 100))},
        ).fetchall()
    ]


def person_details(conn, person_id):
    ensure_lead_followup_schema(conn)
    person = conn.execute(
        """
        SELECT person_id, display_name, primary_email, primary_phone, school,
               resolution_status, source_count, identity_count, raw_json
        FROM persons
        WHERE person_id = ?
        """,
        (person_id,),
    ).fetchone()
    if not person:
        return None
    identities = [
        dict(row)
        for row in conn.execute(
            """
            SELECT identity_type, identity_value, source_system, source_table,
                   source_id, confidence
            FROM person_identities
            WHERE person_id = ?
            ORDER BY identity_type, source_system, source_table, source_id
            LIMIT 200
            """,
            (person_id,),
        ).fetchall()
    ]
    conflicts = [
        dict(row)
        for row in conn.execute(
            """
            SELECT conflict_type, status, evidence_json, created_at
            FROM person_resolution_conflicts
            WHERE person_ids_json LIKE '%' || ? || '%'
            ORDER BY conflict_id
            """,
            (person_id,),
        ).fetchall()
    ]
    return {"person": dict(person), "identities": identities, "conflicts": conflicts}


def _person_ids_for_search(conn, search, limit=5):
    search = str(search or "").strip()
    if not search:
        return []
    direct = conn.execute("SELECT person_id FROM persons WHERE person_id = ?", (search,)).fetchone()
    if direct:
        return [direct["person_id"]]
    return [row["person_id"] for row in person_search(conn, search, limit=limit)]


def person_journey(conn, search, start_date="", end_date="", limit=100, include_sensitive=False):
    ensure_lead_followup_schema(conn)
    person_ids = _person_ids_for_search(conn, search)
    if not person_ids:
        return {"person_ids": [], "events": [], "row_count": 0}
    placeholders = ", ".join(f":person_{index}" for index, _ in enumerate(person_ids))
    params = {f"person_{index}": person_id for index, person_id in enumerate(person_ids)}
    params["limit"] = max(1, min(int(limit), 500))
    date_filters = []
    if start_date:
        params["start_date"] = start_date
        date_filters.append("date(event_at) >= date(:start_date)")
    if end_date:
        params["end_date"] = end_date
        date_filters.append("date(event_at) <= date(:end_date)")
    where_dates = " AND " + " AND ".join(date_filters) if date_filters else ""
    rows = conn.execute(
        f"""
        SELECT person_id, event_at, event_type, source_system, source_id,
               summary, detail_json, school
        FROM vw_person_journey
        WHERE person_id IN ({placeholders})
          AND event_at IS NOT NULL
          {where_dates}
        ORDER BY datetime(event_at), source_system, source_id
        LIMIT :limit
        """,
        params,
    ).fetchall()
    events = []
    for row in rows:
        event = {
            "person_id": row["person_id"],
            "event_at": row["event_at"],
            "event_type": row["event_type"],
            "source_system": row["source_system"],
            "source_id": row["source_id"],
            "summary": row["summary"],
            "school": row["school"],
        }
        if include_sensitive:
            try:
                event["detail"] = json.loads(row["detail_json"] or "{}")
            except json.JSONDecodeError:
                event["detail"] = row["detail_json"]
        events.append(event)
    return {"person_ids": person_ids, "events": events, "row_count": len(events)}


def customer_lifecycle_summary(conn, person_id):
    ensure_lead_followup_schema(conn)
    details = person_details(conn, person_id)
    if not details:
        return None
    rows = conn.execute(
        """
        SELECT event_type, source_system, COUNT(*) AS rows,
               MIN(event_at) AS first_event_at,
               MAX(event_at) AS latest_event_at
        FROM vw_person_journey
        WHERE person_id = ?
        GROUP BY event_type, source_system
        ORDER BY first_event_at, event_type
        """,
        (person_id,),
    ).fetchall()
    first_event = conn.execute(
        """
        SELECT event_at, event_type, source_system, summary
        FROM vw_person_journey
        WHERE person_id = ?
        ORDER BY datetime(event_at), source_system, source_id
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    latest_event = conn.execute(
        """
        SELECT event_at, event_type, source_system, summary
        FROM vw_person_journey
        WHERE person_id = ?
        ORDER BY datetime(event_at) DESC, source_system, source_id
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    event_counts = {f"{row['source_system']}:{row['event_type']}": row["rows"] for row in rows}
    return {
        "person": details["person"],
        "event_count": sum(event_counts.values()),
        "event_counts": event_counts,
        "first_event": dict(first_event) if first_event else None,
        "latest_event": dict(latest_event) if latest_event else None,
        "conflict_count": len(details["conflicts"]),
    }
