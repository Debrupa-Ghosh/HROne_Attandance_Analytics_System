"""
Manual sample database seeder for Employee Attendance & Analytics API.
Run manually via:
    python sample_seed.py

Reads MONGO_URI and MONGO_DB (defaulting to 'attendance_db') from environment variables,
ensures indexes idempotently, and upserts sample_data/employees.json and
sample_data/attendance_logs.json without deleting non-sample records.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient

from app.main import ensure_indexes, normalize_mongo_uri

load_dotenv(override=False)

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = BASE_DIR / "sample_data"


def ms_to_utc_dt(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    truncated_ms = (int(ms) // 1000) * 1000
    return datetime.fromtimestamp(truncated_ms / 1000.0, tz=timezone.utc)


def convert_employee_doc(raw: dict[str, Any]) -> dict[str, Any]:
    doc = dict(raw)
    doc["created_at"] = ms_to_utc_dt(doc["created_at"])
    return doc


def convert_attendance_doc(raw: dict[str, Any]) -> dict[str, Any]:
    doc = dict(raw)
    doc["punch_in"] = ms_to_utc_dt(doc.get("punch_in"))
    doc["punch_out"] = ms_to_utc_dt(doc.get("punch_out"))
    converted_history = []
    for entry in doc.get("history", []):
        h = dict(entry)
        h["at"] = ms_to_utc_dt(h["at"])
        changes = {}
        for field, diff in h.get("changes", {}).items():
            if field in ("punch_in", "punch_out"):
                changes[field] = {
                    "from": ms_to_utc_dt(diff.get("from")),
                    "to": ms_to_utc_dt(diff.get("to")),
                }
            else:
                changes[field] = dict(diff)
        h["changes"] = changes
        converted_history.append(h)
    doc["history"] = converted_history
    return doc


def seed_database(mongo_uri: str | None = None, mongo_db: str | None = None) -> dict[str, int]:
    raw_uri = mongo_uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    uri = normalize_mongo_uri(raw_uri)
    db_name = mongo_db or os.environ.get("MONGO_DB", "attendance_db")
    client = MongoClient(uri, tz_aware=True)
    db = client[db_name]

    ensure_indexes(db)

    employees_raw = json.loads((SAMPLE_DIR / "employees.json").read_text(encoding="utf-8"))
    attendance_raw = json.loads((SAMPLE_DIR / "attendance_logs.json").read_text(encoding="utf-8"))

    emp_docs = [convert_employee_doc(e) for e in employees_raw]
    att_docs = [convert_attendance_doc(a) for a in attendance_raw]

    for doc in emp_docs:
        db.employees.replace_one({"emp_code": doc["emp_code"]}, doc, upsert=True)

    for doc in att_docs:
        db.attendance_logs.replace_one(
            {"emp_code": doc["emp_code"], "date": doc["date"]}, doc, upsert=True
        )

    return {"employees": len(emp_docs), "attendance_logs": len(att_docs)}


if __name__ == "__main__":
    counts = seed_database()
    print(f"Seeded {counts['employees']} employees and {counts['attendance_logs']} attendance logs.")
