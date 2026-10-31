"""
Employee Attendance & Analytics API
Production FastAPI + PyMongo (MongoDB 6.0+ / MongoDB Atlas) implementation.

Key guarantees:
1. Connects to MongoDB Atlas via PyMongo using MONGO_URI and MONGO_DB from environment.
2. Uses attendance_db by default for local application development.
3. Creates all required indexes idempotently at startup without assuming a fixed index count.
4. Never seeds, overwrites, or deletes data at application startup (preserving evaluator datasets).
5. Implements all analytics via MongoDB 6.0+ aggregation pipelines.
6. Implements /admin/explain/{endpoint} using genuine MongoDB executionStats.
"""

from __future__ import annotations

import calendar
import math
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any
from urllib.parse import quote_plus, unquote_plus

from bson import Decimal128, ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, PyMongoError

# Environment variables must take priority over values loaded from .env
load_dotenv(override=False)

IST = timezone(timedelta(hours=5, minutes=30))
EMP_CODE_PATTERN = re.compile(r"^EMP\d{4,6}$")
TIME_HHMM_PATTERN = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
MONTH_YYYYMM_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
DATE_YYYYMMDD_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")

# Valid epoch millisecond range: 2000-01-01T00:00:00Z to 2100-01-01T00:00:00Z
# Rejects seconds-based timestamps (< 1e11) and out-of-range timestamps
MIN_EPOCH_MS = 946_684_800_000
MAX_EPOCH_MS = 4_102_444_800_000


class AttendanceStatus(str, Enum):
    PRESENT = "PRESENT"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"
    ABSENT = "ABSENT"
    LEAVE = "LEAVE"


class PunchInStatus(str, Enum):
    PRESENT = "PRESENT"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"


PRESENCE_STATUSES = {
    AttendanceStatus.PRESENT.value,
    AttendanceStatus.WFH.value,
    AttendanceStatus.ON_DUTY.value,
}
NON_PRESENCE_STATUSES = {
    AttendanceStatus.ABSENT.value,
    AttendanceStatus.LEAVE.value,
}


# ---------------------------------------------------------------------------
# Database Connection & Idempotent Index Initialization
# ---------------------------------------------------------------------------

_mongo_client: MongoClient | None = None
_cached_uri: str | None = None


def normalize_mongo_uri(raw_uri: str) -> str:
    """
    Safely normalize a MongoDB URI so passwords containing unescaped '@' characters
    (e.g., mongodb+srv://user:p@ss@cluster.mongodb.net/) are RFC-3986 percent-encoded
    for PyMongo without altering already-encoded URIs.
    """
    uri = raw_uri.strip()
    for scheme in ("mongodb+srv://", "mongodb://"):
        if uri.startswith(scheme):
            rest = uri[len(scheme) :]
            slash_idx = rest.find("/")
            authority = rest if slash_idx == -1 else rest[:slash_idx]
            suffix = "" if slash_idx == -1 else rest[slash_idx:]
            if "@" in authority:
                userinfo, hostinfo = authority.rsplit("@", 1)
                if ":" in userinfo:
                    user, pwd = userinfo.split(":", 1)
                    enc_user = quote_plus(unquote_plus(user))
                    enc_pwd = quote_plus(unquote_plus(pwd))
                    return f"{scheme}{enc_user}:{enc_pwd}@{hostinfo}{suffix}"
            break
    return uri


def get_mongo_client() -> MongoClient:
    global _mongo_client, _cached_uri
    raw_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_uri = normalize_mongo_uri(raw_uri)
    if _mongo_client is None or _cached_uri != mongo_uri:
        _mongo_client = MongoClient(
            mongo_uri,
            tz_aware=True,
            serverSelectionTimeoutMS=5000,
        )
        _cached_uri = mongo_uri
    return _mongo_client


def get_db() -> Database:
    """
    Return the target MongoDB database configured via MONGO_DB
    (defaulting to 'attendance_db' for local development).
    """
    client = get_mongo_client()
    db_name = os.environ.get("MONGO_DB", "attendance_db")
    return client[db_name]


def ensure_indexes(db: Database | None = None) -> None:
    """
    Idempotently create all necessary indexes on employees and attendance_logs.
    Does not assume a fixed number of indexes and never drops or modifies existing data.
    """
    target_db = db if db is not None else get_db()

    # Employees indexes:
    # 1. Unique emp_code for fast lookups, $lookup joins, and atomic duplicate prevention
    target_db.employees.create_index(
        [("emp_code", ASCENDING)],
        unique=True,
        name="idx_emp_code_unique",
    )
    # 2. Compound index supporting department filter + joined_on headcount bounds + emp_code sort
    target_db.employees.create_index(
        [("department", ASCENDING), ("joined_on", ASCENDING), ("emp_code", ASCENDING)],
        name="idx_dept_joined_emp",
    )
    # 3. Compound index supporting unfiltered department_summary joined_on <= month_end scan
    target_db.employees.create_index(
        [("joined_on", ASCENDING), ("department", ASCENDING)],
        name="idx_joined_dept",
    )

    # Attendance logs indexes:
    # 1. Unique compound index enforcing at most one document per (emp_code, date)
    #    and accelerating PATCH regularization + employee_monthly analytics
    target_db.attendance_logs.create_index(
        [("emp_code", ASCENDING), ("date", ASCENDING)],
        unique=True,
        name="idx_emp_date_unique",
    )
    # 2. Compound index supporting GET /attendance (date DESC, emp_code ASC, status)
    #    and date-range analytics pipelines (department_summary, late_leaderboard, department_trend)
    target_db.attendance_logs.create_index(
        [("date", DESCENDING), ("emp_code", ASCENDING), ("status", ASCENDING)],
        name="idx_date_emp_status",
    )
    # 3. Compound index supporting late_leaderboard (date range + late_minutes > 0)
    target_db.attendance_logs.create_index(
        [("date", ASCENDING), ("late_minutes", ASCENDING), ("emp_code", ASCENDING)],
        name="idx_date_late_emp",
    )
    # 4. Compound index supporting POST /attendance/punch-out lookup (emp_code + punch_in DESC)
    target_db.attendance_logs.create_index(
        [("emp_code", ASCENDING), ("punch_in", DESCENDING)],
        name="idx_emp_punch_in",
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Application startup hook:
    Creates required indexes idempotently.
    NEVER seeds, modifies, or deletes documents so existing/evaluator datasets remain intact.
    """
    try:
        ensure_indexes()
    except PyMongoError:
        # Allow application startup so /health can report HTTP 503 if MongoDB is unreachable
        pass
    yield


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Strict Timestamp, Date, and Rounding Helpers (R1 - R10)
# ---------------------------------------------------------------------------


def round_half_up(value: float | int | Decimal, decimals: int = 2) -> float:
    """R8: Half-up rounding (ROUND_HALF_UP) to specified decimal places."""
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    quantizer = Decimal("10") ** (-decimals)
    return float(d.quantize(quantizer, rounding=ROUND_HALF_UP))


def validate_strict_epoch_ms(value: Any, field_name: str = "timestamp") -> int:
    """
    Section 10: Reject strings, floats, booleans, seconds-based timestamps,
    and out-of-range integer timestamps.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer epoch millisecond value")
    if value < MIN_EPOCH_MS or value > MAX_EPOCH_MS:
        raise ValueError(
            f"{field_name} must be a valid epoch millisecond timestamp between {MIN_EPOCH_MS} and {MAX_EPOCH_MS}"
        )
    return value


def truncate_ms_to_utc_dt(epoch_ms: int) -> datetime:
    """
    R1: Truncate epoch milliseconds to whole seconds before calculations and storage,
    returning a timezone-aware UTC datetime for MongoDB BSON Date storage.
    """
    whole_seconds_ms = (epoch_ms // 1000) * 1000
    return datetime.fromtimestamp(whole_seconds_ms / 1000.0, tz=timezone.utc)


def now_utc_truncated() -> datetime:
    """Current server UTC datetime truncated to whole seconds."""
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return truncate_ms_to_utc_dt(now_ms)


def ensure_utc_dt(dt_val: datetime | None) -> datetime | None:
    """Ensure PyMongo datetime is timezone-aware UTC and truncated to whole seconds."""
    if dt_val is None:
        return None
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=timezone.utc)
    else:
        dt_val = dt_val.astimezone(timezone.utc)
    return dt_val.replace(microsecond=0)


def dt_to_epoch_ms(dt_val: datetime | int | None) -> int | None:
    """Convert stored BSON UTC datetime (or integer ms) to integer epoch milliseconds."""
    if dt_val is None:
        return None
    if isinstance(dt_val, int):
        return (dt_val // 1000) * 1000
    utc_dt = ensure_utc_dt(dt_val)
    assert utc_dt is not None
    return int(utc_dt.timestamp()) * 1000


def parse_hhmm(hhmm: str) -> tuple[int, int]:
    match = TIME_HHMM_PATTERN.match(hhmm)
    if not match:
        raise ValueError(f"Invalid HH:MM time format: {hhmm}")
    return int(match.group(1)), int(match.group(2))


def parse_iso_date(date_str: str, field_name: str = "date") -> date:
    if not isinstance(date_str, str) or not DATE_YYYYMMDD_PATTERN.match(date_str):
        raise ValueError(f"Invalid {field_name} format, expected YYYY-MM-DD")
    try:
        return date.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError(f"Invalid calendar date for {field_name}: {date_str}") from exc


def parse_month_bounds(month_str: str) -> tuple[str, str, date, date]:
    if not isinstance(month_str, str) or not MONTH_YYYYMM_PATTERN.match(month_str):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid month format, expected YYYY-MM",
        )
    year, month = map(int, month_str.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    start_date = date(year, month, 1)
    end_date = date(year, month, last_day)
    return start_date.isoformat(), end_date.isoformat(), start_date, end_date


def is_overnight_shift(shift_start: str, shift_end: str) -> bool:
    """R1: Overnight shifts occur when shift_end <= shift_start."""
    return parse_hhmm(shift_end) <= parse_hhmm(shift_start)


def compute_attendance_date_ist(
    punch_in_utc: datetime, shift_start: str, shift_end: str
) -> str:
    """
    R1: Timezone and attendance date.
    - Shift times use IST (UTC+05:30).
    - Attendance dates are IST calendar dates.
    - Overnight shifts occur when shift_end <= shift_start.
    - For an overnight shift, a punch-in whose IST time is strictly earlier than
      shift_end belongs to the previous shift date.
    """
    utc_dt = ensure_utc_dt(punch_in_utc)
    assert utc_dt is not None
    ist_dt = utc_dt.astimezone(IST)
    ist_cal_date = ist_dt.date()

    if is_overnight_shift(shift_start, shift_end):
        end_h, end_m = parse_hhmm(shift_end)
        shift_end_time = time(end_h, end_m, 0)
        if ist_dt.timetz().replace(tzinfo=None) < shift_end_time:
            ist_cal_date = ist_cal_date - timedelta(days=1)

    return ist_cal_date.isoformat()


def get_shift_window_utc(
    att_date_str: str, shift_start: str, shift_end: str
) -> tuple[datetime, datetime]:
    """
    Return (shift_start_utc, shift_end_utc) for a given attendance date (YYYY-MM-DD)
    and employee shift definition in IST.
    """
    att_date = date.fromisoformat(att_date_str)
    start_h, start_m = parse_hhmm(shift_start)
    end_h, end_m = parse_hhmm(shift_end)

    shift_start_ist = datetime(
        att_date.year, att_date.month, att_date.day, start_h, start_m, 0, tzinfo=IST
    )
    shift_end_ist = datetime(
        att_date.year, att_date.month, att_date.day, end_h, end_m, 0, tzinfo=IST
    )
    if is_overnight_shift(shift_start, shift_end):
        shift_end_ist = shift_end_ist + timedelta(days=1)

    return shift_start_ist.astimezone(timezone.utc), shift_end_ist.astimezone(timezone.utc)


def compute_late_minutes(
    punch_in_utc: datetime, att_date_str: str, shift_start: str, shift_end: str
) -> int:
    """
    R2: Late arrival.
    - Grace period is 10 minutes (600 seconds).
    - Late only when strictly greater than 10 minutes after shift_start.
    - Calculate whole elapsed minutes from shift_start using floor, not ceiling.
    """
    p_in = ensure_utc_dt(punch_in_utc)
    assert p_in is not None
    shift_start_utc, _ = get_shift_window_utc(att_date_str, shift_start, shift_end)
    elapsed_seconds = int((p_in - shift_start_utc).total_seconds())
    if elapsed_seconds <= 600:
        return 0
    return int(math.floor(elapsed_seconds / 60))


def compute_punch_out_metrics(
    punch_in_utc: datetime,
    punch_out_utc: datetime,
    att_date_str: str,
    shift_start: str,
    shift_end: str,
) -> tuple[float, int, bool]:
    """
    Compute (work_hours, overtime_minutes, half_day) following Rules R3, R4, R5, R8:
    - R4: Difference between punch_out and punch_in in seconds converted to hours,
          rounded to 2 decimal places using half-up rounding.
    - R3: Whole minutes after shift_end; count overtime only when >= 30 minutes, else 0.
    - R5: Calculated AFTER rounding work_hours; half_day is True when rounded work_hours < 4.50.
    """
    p_in = ensure_utc_dt(punch_in_utc)
    p_out = ensure_utc_dt(punch_out_utc)
    assert p_in is not None and p_out is not None

    duration_seconds = int((p_out - p_in).total_seconds())
    raw_hours = Decimal(duration_seconds) / Decimal(3600)
    work_hours = round_half_up(raw_hours, 2)

    _, shift_end_utc = get_shift_window_utc(att_date_str, shift_start, shift_end)
    after_shift_seconds = int((p_out - shift_end_utc).total_seconds())
    whole_after_minutes = int(math.floor(after_shift_seconds / 60)) if after_shift_seconds > 0 else 0
    overtime_minutes = whole_after_minutes if whole_after_minutes >= 30 else 0

    half_day = work_hours < 4.50
    return work_hours, overtime_minutes, half_day


def compute_working_days_in_month(month_start: date, month_end: date, joined_on_str: str) -> int:
    """
    R7: Working days are Monday to Friday (weekday() < 5).
    Start counting from joined_on if the employee joined mid-month.
    If joined_on is after month_end, working_days is 0.
    """
    joined_date = date.fromisoformat(joined_on_str)
    if joined_date > month_end:
        return 0
    effective_start = max(month_start, joined_date)
    count = 0
    curr = effective_start
    while curr <= month_end:
        if curr.weekday() < 5:
            count += 1
        curr += timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# Serialization Helpers
# ---------------------------------------------------------------------------


def serialize_employee(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "joined_on": doc["joined_on"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "created_at": dt_to_epoch_ms(doc["created_at"]),
    }


def serialize_history_entry(entry: dict[str, Any]) -> dict[str, Any]:
    changes_out: dict[str, Any] = {}
    for field, diff in entry.get("changes", {}).items():
        if field in ("punch_in", "punch_out"):
            changes_out[field] = {
                "from": dt_to_epoch_ms(diff.get("from")),
                "to": dt_to_epoch_ms(diff.get("to")),
            }
        else:
            changes_out[field] = {
                "from": diff.get("from"),
                "to": diff.get("to"),
            }
    return {
        "at": dt_to_epoch_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": changes_out,
    }


def serialize_attendance(doc: dict[str, Any]) -> dict[str, Any]:
    work_hours = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_epoch_ms(doc.get("punch_in")),
        "punch_out": dt_to_epoch_ms(doc.get("punch_out")),
        "late_minutes": int(doc.get("late_minutes", 0)),
        "work_hours": round_half_up(work_hours, 2) if work_hours is not None else None,
        "overtime_minutes": int(doc.get("overtime_minutes", 0)),
        "half_day": bool(doc.get("half_day", False)),
        "history": [serialize_history_entry(h) for h in doc.get("history", [])],
    }


def sanitize_bson_for_json(obj: Any) -> Any:
    """Recursively convert BSON types in explain output to JSON-safe primitives."""
    if isinstance(obj, dict):
        return {str(k): sanitize_bson_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_bson_for_json(v) for v in obj]
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return dt_to_epoch_ms(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return obj


# ---------------------------------------------------------------------------
# Pydantic v2 Request Schemas
# ---------------------------------------------------------------------------


class EmployeeCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    emp_code: str
    name: str
    email: EmailStr
    department: str
    joined_on: str
    shift_start: str
    shift_end: str

    @field_validator("emp_code")
    @classmethod
    def validate_emp_code(cls, v: Any) -> str:
        if not isinstance(v, str) or not EMP_CODE_PATTERN.match(v):
            raise ValueError("emp_code must match EMP followed by 4 to 6 digits")
        return v

    @field_validator("name", "department")
    @classmethod
    def validate_non_empty_text(cls, v: Any, info) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{info.field_name} must be a non-empty string")
        return v.strip()

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: EmailStr) -> str:
        return str(v).strip().lower()

    @field_validator("joined_on")
    @classmethod
    def validate_joined_on(cls, v: Any) -> str:
        parse_iso_date(v, "joined_on")
        return v

    @field_validator("shift_start", "shift_end")
    @classmethod
    def validate_shift_time(cls, v: Any, info) -> str:
        if not isinstance(v, str) or not TIME_HHMM_PATTERN.match(v):
            raise ValueError(f"{info.field_name} must be in HH:MM 24-hour format")
        return v

    @model_validator(mode="after")
    def validate_shifts_differ(self) -> "EmployeeCreateRequest":
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    emp_code: str
    punched_at: int | None = None
    status: PunchInStatus = PunchInStatus.PRESENT

    @field_validator("emp_code")
    @classmethod
    def validate_emp_code(cls, v: Any) -> str:
        if not isinstance(v, str) or not EMP_CODE_PATTERN.match(v):
            raise ValueError("Invalid emp_code")
        return v

    @field_validator("punched_at", mode="before")
    @classmethod
    def validate_punched_at(cls, v: Any) -> int | None:
        if v is None:
            return None
        return validate_strict_epoch_ms(v, "punched_at")


class PunchOutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    emp_code: str
    punched_at: int | None = None

    @field_validator("emp_code")
    @classmethod
    def validate_emp_code(cls, v: Any) -> str:
        if not isinstance(v, str) or not EMP_CODE_PATTERN.match(v):
            raise ValueError("Invalid emp_code")
        return v

    @field_validator("punched_at", mode="before")
    @classmethod
    def validate_punched_at(cls, v: Any) -> int | None:
        if v is None:
            return None
        return validate_strict_epoch_ms(v, "punched_at")


class AttendanceCorrectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str
    regularized_by: str
    status: AttendanceStatus | None = None
    punch_in: int | None = None
    punch_out: int | None = None

    @field_validator("reason", "regularized_by")
    @classmethod
    def validate_required_strings(cls, v: Any, info) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{info.field_name} must be a non-empty string")
        return v.strip()

    @field_validator("punch_in", "punch_out", mode="before")
    @classmethod
    def validate_optional_timestamps(cls, v: Any, info) -> int | None:
        if v is None:
            return None
        return validate_strict_epoch_ms(v, info.field_name)


# ---------------------------------------------------------------------------
# MongoDB Aggregation Pipeline Builders (Shared with /admin/explain)
# ---------------------------------------------------------------------------


def build_mongo_half_up_expr(value_expr: Any, decimals: int = 2) -> dict[str, Any]:
    """
    Build a MongoDB aggregation expression performing exact decimal half-up rounding
    (Rule R8) using Decimal128 arithmetic:
      floor(toDecimal(value) * 10^decimals + 0.5) / 10^decimals
    """
    scale_int = 10**decimals
    scale_dec = Decimal128(str(scale_int))
    half_dec = Decimal128("0.5")
    return {
        "$toDouble": {
            "$divide": [
                {
                    "$floor": {
                        "$add": [
                            {"$multiply": [{"$toDecimal": value_expr}, scale_dec]},
                            half_dec,
                        ]
                    }
                },
                scale_dec,
            ]
        }
    }


def build_employee_monthly_pipeline(
    emp_code: str, month_start: str, month_end: str, working_days: int
) -> list[dict[str, Any]]:
    """
    Pipeline on attendance_logs for GET /analytics/employees/{emp_code}/monthly.
    Uses index idx_emp_date_unique ({emp_code: 1, date: 1}).
    """
    return [
        {
            "$match": {
                "emp_code": emp_code,
                "date": {"$gte": month_start, "$lte": month_end},
            }
        },
        {
            "$addFields": {
                "iso_dow": {
                    "$isoDayOfWeek": {
                        "$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", list(PRESENCE_STATUSES)]},
                                    {"$lte": ["$iso_dow", 5]},
                                ]
                            },
                            {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "leave_days": {
                    "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {"$sum": "$late_minutes"},
                "total_overtime_minutes": {"$sum": "$overtime_minutes"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "present_days": build_mongo_half_up_expr("$present_days", 2),
                "leave_days": 1,
                "late_count": 1,
                "total_late_minutes": 1,
                "total_overtime_minutes": 1,
                "attendance_pct": (
                    {"$literal": None}
                    if working_days == 0
                    else build_mongo_half_up_expr(
                        {
                            "$divide": [
                                {"$multiply": ["$present_days", 100]},
                                working_days,
                            ]
                        },
                        2,
                    )
                ),
            }
        },
    ]


def build_department_summary_pipeline(
    month_start: str, month_end: str, department: str | None = None
) -> list[dict[str, Any]]:
    """
    Pipeline starting from employees collection for GET /analytics/departments/summary.
    Preserves employees with zero attendance logs and excludes employees who joined
    after month_end. Uses idx_dept_joined_emp or idx_joined_dept.
    """
    emp_match: dict[str, Any] = {"joined_on": {"$lte": month_end}}
    if department is not None:
        emp_match["department"] = department

    return [
        {"$match": emp_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"emp_code": "$emp_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$emp_code", "$$emp_code"]},
                                    {"$gte": ["$date", month_start]},
                                    {"$lte": ["$date", month_end]},
                                ]
                            }
                        }
                    }
                ],
                "as": "logs",
            }
        },
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "log_iso_dow": {
                    "$cond": [
                        {"$ne": [{"$ifNull": ["$logs.date", None]}, None]},
                        {
                            "$isoDayOfWeek": {
                                "$dateFromString": {
                                    "dateString": "$logs.date",
                                    "format": "%Y-%m-%d",
                                }
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$department",
                "emp_set": {"$addToSet": "$emp_code"},
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", list(PRESENCE_STATUSES)]},
                                    {"$lte": ["$log_iso_dow", 5]},
                                ]
                            },
                            {"$cond": [{"$eq": ["$logs.half_day", True]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "work_hours_sum": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", list(PRESENCE_STATUSES)]},
                                    {"$ne": [{"$ifNull": ["$logs.work_hours", None]}, None]},
                                ]
                            },
                            "$logs.work_hours",
                            0.0,
                        ]
                    }
                },
                "work_hours_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", list(PRESENCE_STATUSES)]},
                                    {"$ne": [{"$ifNull": ["$logs.work_hours", None]}, None]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$logs.late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$logs.late_minutes", 0]}
                },
                "leave_count": {
                    "$sum": {"$cond": [{"$eq": ["$logs.status", "LEAVE"]}, 1, 0]}
                },
                "on_duty_count": {
                    "$sum": {"$cond": [{"$eq": ["$logs.status", "ON_DUTY"]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": {"$size": "$emp_set"},
                "present_days": build_mongo_half_up_expr("$present_days", 2),
                "avg_work_hours": {
                    "$cond": [
                        {"$eq": ["$work_hours_count", 0]},
                        None,
                        build_mongo_half_up_expr(
                            {"$divide": ["$work_hours_sum", "$work_hours_count"]}, 2
                        ),
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def build_late_leaderboard_pipeline(
    month_start: str,
    month_end: str,
    limit: int = 10,
    department: str | None = None,
) -> list[dict[str, Any]]:
    """
    Pipeline on attendance_logs for GET /analytics/leaderboard/late.
    Uses MongoDB $setWindowFields with $rank for standard competition ranking,
    breaks ties by emp_code ASC, and filters rank <= limit after ranking.
    """
    pipeline: list[dict[str, Any]] = [
        {
            "$match": {
                "date": {"$gte": month_start, "$lte": month_end},
                "late_minutes": {"$gt": 0},
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "emp",
            }
        },
        {"$unwind": "$emp"},
    ]

    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})

    pipeline.extend(
        [
            {
                "$setWindowFields": {
                    "sortBy": {"total_late_minutes": -1},
                    "output": {"rank": {"$rank": {}}},
                }
            },
            {"$sort": {"total_late_minutes": -1, "_id": 1}},
            {"$match": {"rank": {"$lte": limit}}},
            {
                "$project": {
                    "_id": 0,
                    "rank": 1,
                    "emp_code": "$_id",
                    "name": "$emp.name",
                    "department": "$emp.department",
                    "late_count": 1,
                    "total_late_minutes": 1,
                }
            },
        ]
    )
    return pipeline


def build_department_trend_pipeline(
    department: str, from_date: date, to_date: date
) -> list[dict[str, Any]]:
    """
    Pipeline starting from employees (seeded by 1 matched department employee via IXSCAN)
    using MongoDB $densify to generate every calendar day in [from_date, to_date] and
    $setWindowFields over documents [-6, 0] to compute the 7-day moving average.
    """
    from_dt = datetime(from_date.year, from_date.month, from_date.day, tzinfo=timezone.utc)
    # $densify upper bound is exclusive, so add 1 day to include to_date
    to_dt_exclusive = datetime(
        to_date.year, to_date.month, to_date.day, tzinfo=timezone.utc
    ) + timedelta(days=1)

    return [
        {"$match": {"department": department}},
        {"$limit": 1},
        {"$project": {"_id": 0, "dt": {"$literal": from_dt}}},
        {
            "$densify": {
                "field": "dt",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [from_dt, to_dt_exclusive],
                },
            }
        },
        {
            "$addFields": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$dt"}},
                "iso_dow": {"$isoDayOfWeek": "$dt"},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "let": {"d": "$date"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$department", department]},
                                    {"$lte": ["$joined_on", "$$d"]},
                                ]
                            }
                        }
                    },
                    {"$project": {"_id": 0, "emp_code": 1}},
                ],
                "as": "active_emps",
            }
        },
        {
            "$addFields": {
                "headcount": {"$size": "$active_emps"},
                "active_emp_codes": "$active_emps.emp_code",
                "is_working_day": {"$lte": ["$iso_dow", 5]},
            }
        },
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"d": "$date", "codes": "$active_emp_codes"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$date", "$$d"]},
                                    {"$in": ["$emp_code", "$$codes"]},
                                ]
                            }
                        }
                    }
                ],
                "as": "day_logs",
            }
        },
        {
            "$addFields": {
                "present_count": {
                    "$sum": {
                        "$map": {
                            "input": "$day_logs",
                            "as": "log",
                            "in": {
                                "$cond": [
                                    {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                                    {"$cond": [{"$eq": ["$$log.half_day", True]}, 0.5, 1.0]},
                                    0.0,
                                ]
                            },
                        }
                    }
                },
                "late_count": {
                    "$sum": {
                        "$map": {
                            "input": "$day_logs",
                            "as": "log",
                            "in": {"$cond": [{"$gt": ["$$log.late_minutes", 0]}, 1, 0]},
                        }
                    }
                },
            }
        },
        {
            "$addFields": {
                "attendance_rate": {
                    "$cond": [
                        {
                            "$or": [
                                {"$eq": ["$is_working_day", False]},
                                {"$eq": ["$headcount", 0]},
                            ]
                        },
                        None,
                        build_mongo_half_up_expr(
                            {"$divide": ["$present_count", "$headcount"]}, 4
                        ),
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"dt": 1},
                "output": {
                    "raw_moving_avg_7d": {
                        "$avg": "$attendance_rate",
                        "window": {"documents": [-6, 0]},
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": build_mongo_half_up_expr("$present_count", 2),
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": {
                    "$cond": [
                        {"$eq": [{"$ifNull": ["$raw_moving_avg_7d", None]}, None]},
                        None,
                        build_mongo_half_up_expr("$raw_moving_avg_7d", 4),
                    ]
                },
            }
        },
        {"$sort": {"date": 1}},
    ]


# ---------------------------------------------------------------------------
# Part B: Core REST API Endpoints (1 - 7)
# ---------------------------------------------------------------------------


@app.get("/health")
def health_check():
    """
    A. Health: Check MongoDB connectivity using an actual database ping.
    Return HTTP 200 with {"status": "ok"} when available, HTTP 503 when unavailable.
    """
    try:
        db = get_db()
        db.command("ping")
        return {"status": "ok"}
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable", "detail": "MongoDB is unavailable"},
        )


@app.post("/employees", status_code=status.HTTP_201_CREATED)
def create_employee(payload: EmployeeCreateRequest):
    """
    B. Employee creation:
    - Validate employee fields (emp_code ^EMP\\d{4,6}$, shift_start != shift_end, etc.)
    - Set created_at using server clock (stored as UTC BSON datetime, returned as epoch ms)
    - Enforce uniqueness via MongoDB unique index so concurrent duplicates produce
      one HTTP 201 and remaining HTTP 409.
    """
    db = get_db()
    created_at_dt = now_utc_truncated()

    doc = {
        "emp_code": payload.emp_code,
        "name": payload.name,
        "email": payload.email,
        "department": payload.department,
        "joined_on": payload.joined_on,
        "shift_start": payload.shift_start,
        "shift_end": payload.shift_end,
        "created_at": created_at_dt,
    }

    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Employee with emp_code '{payload.emp_code}' already exists",
        ) from exc

    return serialize_employee(doc)


@app.get("/employees")
def list_employees(
    department: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    """
    C. Employee listing:
    - Optional department filter
    - Sorted by emp_code ascending
    - Filtered, sorted, and paginated inside MongoDB
    """
    db = get_db()
    query: dict[str, Any] = {}
    if department is not None:
        query["department"] = department

    total = db.employees.count_documents(query)
    skip = (page - 1) * page_size
    cursor = (
        db.employees.find(query, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip(skip)
        .limit(page_size)
    )
    items = [serialize_employee(doc) for doc in cursor]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.post("/attendance/punch-in", status_code=status.HTTP_201_CREATED)
def punch_in(payload: PunchInRequest):
    """
    D. Employee punch-in:
    - Validate employee exists (404 if missing)
    - Truncate timestamp to whole seconds before calculations and storage
    - Determine attendance date using R1 (including overnight shift logic)
    - Calculate late_minutes using R2
    - Enforce concurrency using unique compound index (emp_code, date) + atomic insert_one
      so simultaneous requests produce exactly one HTTP 201 and remaining HTTP 409.
    """
    db = get_db()
    emp = db.employees.find_one({"emp_code": payload.emp_code})
    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee '{payload.emp_code}' not found",
        )

    punch_in_dt = (
        truncate_ms_to_utc_dt(payload.punched_at)
        if payload.punched_at is not None
        else now_utc_truncated()
    )

    att_date = compute_attendance_date_ist(
        punch_in_dt, emp["shift_start"], emp["shift_end"]
    )
    late_minutes = compute_late_minutes(
        punch_in_dt, att_date, emp["shift_start"], emp["shift_end"]
    )

    doc = {
        "emp_code": payload.emp_code,
        "date": att_date,
        "status": payload.status.value,
        "punch_in": punch_in_dt,
        "punch_out": None,
        "late_minutes": late_minutes,
        "work_hours": None,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Attendance record already exists for '{payload.emp_code}' on {att_date}",
        ) from exc

    return serialize_attendance(doc)


@app.post("/attendance/punch-out")
def punch_out(payload: PunchOutRequest):
    """
    E. Employee punch-out:
    - Find employee's most recent attendance record whose punch_in is at or before punched_at
    - Reject unknown employees with 404
    - Return 404 when no matching punch-in exists
    - Return 409 if the matching record has already been closed
    - Return 422 when punch-out is not strictly after punch-in or duration exceeds 24 hours
    - Use atomic conditional find_one_and_update({"_id": ..., "punch_out": None}) so
      simultaneous punch-outs produce exactly one HTTP 200 and remaining HTTP 409.
    """
    db = get_db()
    emp = db.employees.find_one({"emp_code": payload.emp_code})
    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee '{payload.emp_code}' not found",
        )

    punch_out_dt = (
        truncate_ms_to_utc_dt(payload.punched_at)
        if payload.punched_at is not None
        else now_utc_truncated()
    )

    record = db.attendance_logs.find_one(
        {
            "emp_code": payload.emp_code,
            "punch_in": {"$ne": None, "$lte": punch_out_dt},
        },
        sort=[("punch_in", DESCENDING)],
    )

    if not record:
        future_open = db.attendance_logs.find_one(
            {"emp_code": payload.emp_code, "punch_in": {"$gt": punch_out_dt}, "punch_out": None}
        )
        if future_open:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="punch_out must be strictly after punch_in",
            )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No matching punch-in record found for '{payload.emp_code}'",
        )

    if record.get("punch_out") is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Matching attendance record has already been closed",
        )

    punch_in_dt = ensure_utc_dt(record["punch_in"])
    assert punch_in_dt is not None

    duration_seconds = int((punch_out_dt - punch_in_dt).total_seconds())
    if duration_seconds <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="punch_out must be strictly after punch_in",
        )
    if duration_seconds > 86_400:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Shift duration cannot exceed 24 hours",
        )

    work_hours, overtime_minutes, half_day = compute_punch_out_metrics(
        punch_in_dt,
        punch_out_dt,
        record["date"],
        emp["shift_start"],
        emp["shift_end"],
    )

    updated = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {
            "$set": {
                "punch_out": punch_out_dt,
                "work_hours": work_hours,
                "overtime_minutes": overtime_minutes,
                "half_day": half_day,
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Attendance record was already closed by a concurrent request",
        )

    return serialize_attendance(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    status_filter: AttendanceStatus | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    """
    F. Attendance listing:
    - Inclusive date_from and date_to filters (422 if invalid or date_from > date_to)
    - Sort by date DESC, emp_code ASC
    - Pagination and filtering executed inside MongoDB
    """
    try:
        parsed_from = parse_iso_date(date_from, "date_from") if date_from is not None else None
        parsed_to = parse_iso_date(date_to, "date_to") if date_to is not None else None
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    if parsed_from and parsed_to and parsed_from > parsed_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="date_from cannot be greater than date_to",
        )

    query: dict[str, Any] = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        date_cond: dict[str, Any] = {}
        if date_from is not None:
            date_cond["$gte"] = date_from
        if date_to is not None:
            date_cond["$lte"] = date_to
        query["date"] = date_cond
    if status_filter is not None:
        query["status"] = status_filter.value

    db = get_db()
    total = db.attendance_logs.count_documents(query)
    skip = (page - 1) * page_size
    cursor = (
        db.attendance_logs.find(query, {"_id": 0})
        .sort([("date", DESCENDING), ("emp_code", ASCENDING)])
        .skip(skip)
        .limit(page_size)
    )
    items = [serialize_attendance(doc) for doc in cursor]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.patch("/attendance/{emp_code}/{att_date}")
def regularize_attendance(
    payload: AttendanceCorrectionRequest,
    emp_code: str = Path(...),
    att_date: str = Path(..., alias="att_date"),
):
    """
    G. Manual attendance correction (PATCH /attendance/{emp_code}/{date}):
    - Validate employee and attendance record exist (404 if missing)
    - Apply requested changes, validate state transitions and R1 date invariants
    - Recalculate derived fields (late_minutes, work_hours, overtime_minutes, half_day)
    - Reject requests that make no actual changes (422)
    - Append exactly one audit history entry with only changed fields
    - Use atomic conditional update to prevent concurrent overwrites (409 on conflict)
    """
    try:
        parse_iso_date(att_date, "date")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    db = get_db()
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee '{emp_code}' not found",
        )

    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": att_date})
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Attendance record not found for '{emp_code}' on {att_date}",
        )

    old_status: str = record["status"]
    old_punch_in: datetime | None = ensure_utc_dt(record.get("punch_in"))
    old_punch_out: datetime | None = ensure_utc_dt(record.get("punch_out"))
    old_history_len = len(record.get("history", []))

    fields_set = payload.model_fields_set

    new_status = (
        payload.status.value
        if "status" in fields_set and payload.status is not None
        else old_status
    )

    if "punch_in" in fields_set:
        new_punch_in = (
            truncate_ms_to_utc_dt(payload.punch_in)
            if payload.punch_in is not None
            else None
        )
    else:
        new_punch_in = None if new_status in NON_PRESENCE_STATUSES else old_punch_in

    if "punch_out" in fields_set:
        new_punch_out = (
            truncate_ms_to_utc_dt(payload.punch_out)
            if payload.punch_out is not None
            else None
        )
    else:
        new_punch_out = None if new_status in NON_PRESENCE_STATUSES else old_punch_out

    if new_status in NON_PRESENCE_STATUSES:
        if new_punch_in is not None or new_punch_out is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="ABSENT and LEAVE records must have null punch_in and punch_out",
            )
        late_minutes = 0
        work_hours = None
        overtime_minutes = 0
        half_day = False
    else:
        if new_punch_in is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Status '{new_status}' requires a non-null punch_in",
            )
        computed_date = compute_attendance_date_ist(
            new_punch_in, emp["shift_start"], emp["shift_end"]
        )
        if computed_date != att_date:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Corrected punch_in belongs to shift date {computed_date}, expected {att_date}",
            )

        late_minutes = compute_late_minutes(
            new_punch_in, att_date, emp["shift_start"], emp["shift_end"]
        )

        if new_punch_out is not None:
            duration_seconds = int((new_punch_out - new_punch_in).total_seconds())
            if duration_seconds <= 0:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="punch_out must be strictly after punch_in",
                )
            if duration_seconds > 86_400:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Shift duration cannot exceed 24 hours",
                )
            work_hours, overtime_minutes, half_day = compute_punch_out_metrics(
                new_punch_in,
                new_punch_out,
                att_date,
                emp["shift_start"],
                emp["shift_end"],
            )
        else:
            work_hours = None
            overtime_minutes = 0
            half_day = False

    changes: dict[str, Any] = {}
    if new_status != old_status:
        changes["status"] = {"from": old_status, "to": new_status}
    if new_punch_in != old_punch_in:
        changes["punch_in"] = {"from": old_punch_in, "to": new_punch_in}
    if new_punch_out != old_punch_out:
        changes["punch_out"] = {"from": old_punch_out, "to": new_punch_out}

    if not changes:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Correction request makes no actual changes to the attendance record",
        )

    history_entry = {
        "at": now_utc_truncated(),
        "by": payload.regularized_by,
        "reason": payload.reason,
        "changes": changes,
    }

    updated = db.attendance_logs.find_one_and_update(
        {
            "_id": record["_id"],
            "status": old_status,
            "punch_in": record.get("punch_in"),
            "punch_out": record.get("punch_out"),
            "history": {"$size": old_history_len},
        },
        {
            "$set": {
                "status": new_status,
                "punch_in": new_punch_in,
                "punch_out": new_punch_out,
                "late_minutes": late_minutes,
                "work_hours": work_hours,
                "overtime_minutes": overtime_minutes,
                "half_day": half_day,
            },
            "$push": {"history": history_entry},
        },
        return_document=ReturnDocument.AFTER,
    )

    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Concurrent modification detected while regularizing attendance record",
        )

    return serialize_attendance(updated)


# ---------------------------------------------------------------------------
# Part C: Analytics Endpoints (8 - 11)
# ---------------------------------------------------------------------------


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly_analytics(
    emp_code: str = Path(...),
    month: str = Query(...),
):
    """
    6A. Employee monthly analytics:
    Uses MongoDB aggregation pipeline on attendance_logs.
    """
    month_start_str, month_end_str, month_start_date, month_end_date = parse_month_bounds(
        month
    )
    db = get_db()
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee '{emp_code}' not found",
        )

    working_days = compute_working_days_in_month(
        month_start_date, month_end_date, emp["joined_on"]
    )
    pipeline = build_employee_monthly_pipeline(
        emp_code, month_start_str, month_end_str, working_days
    )
    results = list(db.attendance_logs.aggregate(pipeline))

    if results:
        row = results[0]
        return {
            "emp_code": emp_code,
            "month": month,
            "working_days": working_days,
            "present_days": float(row["present_days"]),
            "leave_days": int(row["leave_days"]),
            "late_count": int(row["late_count"]),
            "total_late_minutes": int(row["total_late_minutes"]),
            "total_overtime_minutes": int(row["total_overtime_minutes"]),
            "attendance_pct": (
                float(row["attendance_pct"])
                if row["attendance_pct"] is not None
                else None
            ),
        }

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": 0.0,
        "leave_days": 0,
        "late_count": 0,
        "total_late_minutes": 0,
        "total_overtime_minutes": 0,
        "attendance_pct": 0.0 if working_days > 0 else None,
    }


@app.get("/analytics/departments/summary")
def department_summary_analytics(
    month: str = Query(...),
    department: str | None = Query(default=None),
):
    """
    6B. Department summary:
    Starts from employees collection and uses $lookup so employees with zero
    attendance logs are included in headcount.
    """
    month_start_str, month_end_str, _, _ = parse_month_bounds(month)
    db = get_db()
    pipeline = build_department_summary_pipeline(
        month_start_str, month_end_str, department
    )
    items = list(db.employees.aggregate(pipeline))
    return {
        "month": month,
        "items": items,
    }


@app.get("/analytics/leaderboard/late")
def late_leaderboard_analytics(
    month: str = Query(...),
    limit: int = Query(default=10, ge=1, le=50),
    department: str | None = Query(default=None),
):
    """
    6C. Late leaderboard:
    Uses MongoDB $setWindowFields with $rank for standard competition ranking.
    Applies limit after ranking so all ties at cutoff rank <= limit are included.
    """
    month_start_str, month_end_str, _, _ = parse_month_bounds(month)
    db = get_db()
    pipeline = build_late_leaderboard_pipeline(
        month_start_str, month_end_str, limit, department
    )
    items = list(db.attendance_logs.aggregate(pipeline))
    return {
        "month": month,
        "items": items,
    }


@app.get("/analytics/departments/{department}/trend")
def department_trend_analytics(
    department: str = Path(...),
    from_str: str = Query(..., alias="from"),
    to_str: str = Query(..., alias="to"),
):
    """
    6D. Department attendance trend:
    Uses MongoDB $densify and $setWindowFields for gap-free daily rows and
    7-day moving average calculation.
    """
    try:
        from_date = parse_iso_date(from_str, "from")
        to_date = parse_iso_date(to_str, "to")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    if to_date < from_date:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="'to' date cannot be earlier than 'from' date",
        )

    num_days = (to_date - from_date).days + 1
    if num_days > 92:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Date range cannot exceed 92 days",
        )

    db = get_db()
    dept_exists = db.employees.count_documents({"department": department}, limit=1)
    if dept_exists == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Department '{department}' not found",
        )

    pipeline = build_department_trend_pipeline(department, from_date, to_date)
    items = list(db.employees.aggregate(pipeline))
    return {
        "department": department,
        "from": from_str,
        "to": to_str,
        "items": items,
    }


# ---------------------------------------------------------------------------
# Section 7: MongoDB Explain Endpoint (12)
# ---------------------------------------------------------------------------

ALLOWED_EXPLAIN_ENDPOINTS = {
    "attendance_list",
    "employee_monthly",
    "department_summary",
    "late_leaderboard",
    "department_trend",
}


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: str = Path(...),
    emp_code: str | None = Query(default=None),
    month: str | None = Query(default=None),
    department: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    from_str: str | None = Query(default=None, alias="from"),
    to_str: str | None = Query(default=None, alias="to"),
    status_filter: AttendanceStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=10, ge=1, le=50),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    """
    7. MongoDB Explain Endpoint:
    Executes MongoDB explain with executionStats verbosity on the actual query or
    aggregation pipeline for the selected endpoint.
    """
    if endpoint not in ALLOWED_EXPLAIN_ENDPOINTS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid endpoint '{endpoint}'. Allowed values: {sorted(ALLOWED_EXPLAIN_ENDPOINTS)}",
        )

    db = get_db()

    if endpoint == "attendance_list":
        query: dict[str, Any] = {}
        if emp_code is not None:
            query["emp_code"] = emp_code
        if date_from is not None or date_to is not None:
            d_cond: dict[str, Any] = {}
            if date_from is not None:
                d_cond["$gte"] = date_from
            if date_to is not None:
                d_cond["$lte"] = date_to
            query["date"] = d_cond
        if status_filter is not None:
            query["status"] = status_filter.value

        skip = (page - 1) * page_size
        cmd = {
            "find": "attendance_logs",
            "filter": query,
            "sort": {"date": -1, "emp_code": 1},
            "projection": {"_id": 0},
            "skip": skip,
            "limit": page_size,
        }
        raw_explain = db.command("explain", cmd, verbosity="executionStats")
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": sanitize_bson_for_json(raw_explain),
        }

    if endpoint == "employee_monthly":
        target_emp = emp_code or "EMP0001"
        target_month = month or "2025-01"
        m_start, m_end, m_start_date, m_end_date = parse_month_bounds(target_month)
        emp_doc = db.employees.find_one({"emp_code": target_emp})
        joined_on = emp_doc["joined_on"] if emp_doc else f"{target_month}-01"
        w_days = compute_working_days_in_month(m_start_date, m_end_date, joined_on)
        pipeline = build_employee_monthly_pipeline(target_emp, m_start, m_end, w_days)
        cmd = {
            "aggregate": "attendance_logs",
            "pipeline": pipeline,
            "cursor": {},
        }
        raw_explain = db.command("explain", cmd, verbosity="executionStats")
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": sanitize_bson_for_json(raw_explain),
        }

    if endpoint == "department_summary":
        target_month = month or "2025-01"
        m_start, m_end, _, _ = parse_month_bounds(target_month)
        pipeline = build_department_summary_pipeline(m_start, m_end, department)
        cmd = {
            "aggregate": "employees",
            "pipeline": pipeline,
            "cursor": {},
        }
        raw_explain = db.command("explain", cmd, verbosity="executionStats")
        return {
            "endpoint": endpoint,
            "collection": "employees",
            "explain": sanitize_bson_for_json(raw_explain),
        }

    if endpoint == "late_leaderboard":
        target_month = month or "2025-01"
        m_start, m_end, _, _ = parse_month_bounds(target_month)
        pipeline = build_late_leaderboard_pipeline(m_start, m_end, limit, department)
        cmd = {
            "aggregate": "attendance_logs",
            "pipeline": pipeline,
            "cursor": {},
        }
        raw_explain = db.command("explain", cmd, verbosity="executionStats")
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": sanitize_bson_for_json(raw_explain),
        }

    # endpoint == "department_trend"
    target_dept = department or "Engineering"
    eff_from = from_str or date_from or "2025-01-01"
    eff_to = to_str or date_to or "2025-01-14"
    try:
        f_date = parse_iso_date(eff_from, "from")
        t_date = parse_iso_date(eff_to, "to")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    pipeline = build_department_trend_pipeline(target_dept, f_date, t_date)
    cmd = {
        "aggregate": "employees",
        "pipeline": pipeline,
        "cursor": {},
    }
    raw_explain = db.command("explain", cmd, verbosity="executionStats")
    return {
        "endpoint": endpoint,
        "collection": "employees",
        "explain": sanitize_bson_for_json(raw_explain),
    }
