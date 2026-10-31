"""
Comprehensive automated pytest suite for the Employee Attendance & Analytics API.
Configured to strictly use an isolated test database ('attendance_test_db') on the
configured MongoDB cluster so application and evaluator data in 'attendance_db'
are never modified or deleted.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest
from dotenv import load_dotenv
from fastapi.testclient import TestClient

load_dotenv(override=False)

# Strictly isolate pytest to attendance_test_db on the same cluster
TEST_DB_NAME = os.environ.get("MONGO_TEST_DB", "attendance_test_db")
if TEST_DB_NAME == "attendance_db":
    TEST_DB_NAME = "attendance_test_db"
os.environ["MONGO_DB"] = TEST_DB_NAME

from app.main import IST, app, ensure_indexes, get_db  # noqa: E402


def ist_to_ms(year: int, month: int, day: int, hour: int, minute: int, second: int = 0) -> int:
    dt = datetime(year, month, day, hour, minute, second, tzinfo=IST)
    return int(dt.timestamp()) * 1000


@pytest.fixture(autouse=True)
def isolated_test_db():
    """
    Ensure every test runs against the isolated attendance_test_db database.
    Safety guard refuses to clean any database that is not explicitly a test database.
    """
    os.environ["MONGO_DB"] = TEST_DB_NAME
    db = get_db()
    assert db.name == TEST_DB_NAME and "test" in db.name.lower(), (
        f"Refusing to run destructive test fixture against non-test database: {db.name}"
    )
    db.employees.delete_many({})
    db.attendance_logs.delete_many({})
    ensure_indexes(db)
    yield db
    db.employees.delete_many({})
    db.attendance_logs.delete_many({})


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def create_sample_emp(
    client: TestClient,
    emp_code: str = "EMP0001",
    name: str = "Aarav Sharma",
    department: str = "Engineering",
    joined_on: str = "2024-06-01",
    shift_start: str = "09:00",
    shift_end: str = "18:00",
):
    resp = client.post(
        "/employees",
        json={
            "emp_code": emp_code,
            "name": name,
            "email": f"{emp_code.lower()}@acme.internal",
            "department": department,
            "joined_on": joined_on,
            "shift_start": shift_start,
            "shift_end": shift_end,
        },
    )
    assert resp.status_code == 201
    return resp.json()


def test_00_health_endpoint(client: TestClient):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_01_duplicate_employee_creation(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001")
    dup = client.post(
        "/employees",
        json={
            "emp_code": "EMP0001",
            "name": "Duplicate User",
            "email": "dup@acme.internal",
            "department": "Engineering",
            "joined_on": "2024-06-01",
            "shift_start": "09:00",
            "shift_end": "18:00",
        },
    )
    assert dup.status_code == 409


def test_02_concurrent_employee_creation(client: TestClient):
    payload = {
        "emp_code": "EMP0099",
        "name": "Concurrent Worker",
        "email": "concurrent@acme.internal",
        "department": "Engineering",
        "joined_on": "2024-01-10",
        "shift_start": "09:00",
        "shift_end": "18:00",
    }

    def attempt_create(_i: int) -> int:
        with TestClient(app) as c:
            return c.post("/employees", json=payload).status_code

    with ThreadPoolExecutor(max_workers=6) as pool:
        codes = list(pool.map(attempt_create, range(6)))

    assert codes.count(201) == 1
    assert codes.count(409) == 5


def test_03_duplicate_punch_in(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001")
    ts = ist_to_ms(2025, 1, 2, 9, 0, 0)
    r1 = client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ts})
    assert r1.status_code == 201
    r2 = client.post(
        "/attendance/punch-in",
        json={"emp_code": "EMP0001", "punched_at": ts + 120_000},
    )
    assert r2.status_code == 409


def test_04_concurrent_punch_in(client: TestClient):
    create_sample_emp(client, emp_code="EMP0002")
    ts = ist_to_ms(2025, 1, 3, 9, 5, 0)

    def attempt_punch(_i: int) -> int:
        with TestClient(app) as c:
            return c.post(
                "/attendance/punch-in",
                json={"emp_code": "EMP0002", "punched_at": ts},
            ).status_code

    with ThreadPoolExecutor(max_workers=6) as pool:
        codes = list(pool.map(attempt_punch, range(6)))

    assert codes.count(201) == 1
    assert codes.count(409) == 5


def test_05_punch_out_without_punch_in(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001")
    resp = client.post(
        "/attendance/punch-out",
        json={"emp_code": "EMP0001", "punched_at": ist_to_ms(2025, 1, 2, 18, 0, 0)},
    )
    assert resp.status_code == 404


def test_06_concurrent_punch_out(client: TestClient):
    create_sample_emp(client, emp_code="EMP0003")
    p_in = ist_to_ms(2025, 1, 2, 9, 0, 0)
    p_out = ist_to_ms(2025, 1, 2, 18, 0, 0)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0003", "punched_at": p_in})

    def attempt_out(_i: int) -> int:
        with TestClient(app) as c:
            return c.post(
                "/attendance/punch-out",
                json={"emp_code": "EMP0003", "punched_at": p_out},
            ).status_code

    with ThreadPoolExecutor(max_workers=6) as pool:
        codes = list(pool.map(attempt_out, range(6)))

    assert codes.count(200) == 1
    assert codes.count(409) == 5


def test_07_overnight_shifts(client: TestClient):
    create_sample_emp(
        client,
        emp_code="EMP0004",
        department="Operations",
        shift_start="22:00",
        shift_end="06:00",
    )
    # Punch in at 00:30 IST on 2025-01-03 (< 06:00 shift_end) -> belongs to 2025-01-02 shift!
    p_in = ist_to_ms(2025, 1, 3, 0, 30, 0)
    r_in = client.post("/attendance/punch-in", json={"emp_code": "EMP0004", "punched_at": p_in})
    assert r_in.status_code == 201
    data_in = r_in.json()
    assert data_in["date"] == "2025-01-02"
    assert data_in["late_minutes"] == 150

    # Punch out at 06:45 IST on 2025-01-03 (45 mins after 06:00 shift_end)
    p_out = ist_to_ms(2025, 1, 3, 6, 45, 0)
    r_out = client.post("/attendance/punch-out", json={"emp_code": "EMP0004", "punched_at": p_out})
    assert r_out.status_code == 200
    data_out = r_out.json()
    assert data_out["work_hours"] == 6.25
    assert data_out["overtime_minutes"] == 45
    assert data_out["half_day"] is False


def test_08_late_arrival_at_exact_10_minutes(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    p_in = ist_to_ms(2025, 1, 2, 9, 10, 0)
    resp = client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})
    assert resp.status_code == 201
    assert resp.json()["late_minutes"] == 0


def test_09_late_arrival_at_10_minutes_1_second(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    p_in = ist_to_ms(2025, 1, 2, 9, 10, 1)
    resp = client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})
    assert resp.status_code == 201
    assert resp.json()["late_minutes"] == 10


def test_10_overtime_below_30_minutes(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    p_in = ist_to_ms(2025, 1, 2, 9, 0, 0)
    p_out = ist_to_ms(2025, 1, 2, 18, 29, 59)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})
    resp = client.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": p_out})
    assert resp.status_code == 200
    assert resp.json()["overtime_minutes"] == 0


def test_11_overtime_exactly_30_minutes(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    p_in = ist_to_ms(2025, 1, 2, 9, 0, 0)
    p_out = ist_to_ms(2025, 1, 2, 18, 30, 0)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})
    resp = client.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": p_out})
    assert resp.status_code == 200
    assert resp.json()["overtime_minutes"] == 30


def test_12_half_day_calculations_and_half_up_rounding(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    # 16182 seconds = 4.495 hours -> half-up rounds to 4.50 -> NOT a half day (4.50 < 4.50 is False)
    p_in_1 = ist_to_ms(2025, 1, 2, 9, 0, 0)
    p_out_1 = p_in_1 + (16_182 * 1000)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in_1})
    r1 = client.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": p_out_1})
    assert r1.status_code == 200
    assert r1.json()["work_hours"] == 4.5
    assert r1.json()["half_day"] is False

    # 16164 seconds = 4.49 hours -> half_day is True
    p_in_2 = ist_to_ms(2025, 1, 3, 9, 0, 0)
    p_out_2 = p_in_2 + (16_164 * 1000)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in_2})
    r2 = client.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": p_out_2})
    assert r2.status_code == 200
    assert r2.json()["work_hours"] == 4.49
    assert r2.json()["half_day"] is True


def test_13_manual_correction_and_audit_history(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    p_in = ist_to_ms(2025, 1, 6, 9, 25, 0)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})

    corrected_in = ist_to_ms(2025, 1, 6, 9, 0, 0)
    corrected_out = ist_to_ms(2025, 1, 6, 18, 30, 0)
    resp = client.patch(
        "/attendance/EMP0001/2025-01-06",
        json={
            "reason": "Biometric terminal clock drift regularization",
            "regularized_by": "EMP0007",
            "status": "ON_DUTY",
            "punch_in": corrected_in,
            "punch_out": corrected_out,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ON_DUTY"
    assert body["late_minutes"] == 0
    assert body["work_hours"] == 9.5
    assert body["overtime_minutes"] == 30
    assert len(body["history"]) == 1
    h = body["history"][0]
    assert h["by"] == "EMP0007"
    assert h["changes"]["status"] == {"from": "PRESENT", "to": "ON_DUTY"}
    assert h["changes"]["punch_in"] == {"from": p_in, "to": corrected_in}
    assert h["changes"]["punch_out"] == {"from": None, "to": corrected_out}

    noop = client.patch(
        "/attendance/EMP0001/2025-01-06",
        json={
            "reason": "Redundant request",
            "regularized_by": "EMP0007",
            "status": "ON_DUTY",
        },
    )
    assert noop.status_code == 422


def test_14_concurrent_manual_corrections(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", shift_start="09:00", shift_end="18:00")
    p_in = ist_to_ms(2025, 1, 7, 9, 20, 0)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})

    def attempt_patch(idx: int) -> int:
        with TestClient(app) as c:
            return c.patch(
                "/attendance/EMP0001/2025-01-07",
                json={
                    "reason": f"Concurrent regularization {idx}",
                    "regularized_by": "EMP0007",
                    "punch_out": ist_to_ms(2025, 1, 7, 17, 30 + idx, 0),
                },
            ).status_code

    with ThreadPoolExecutor(max_workers=5) as pool:
        codes = list(pool.map(attempt_patch, range(5)))

    assert 200 in codes
    rec = client.get("/attendance", params={"emp_code": "EMP0001"}).json()["items"][0]
    assert len(rec["history"]) == codes.count(200)


def test_15_employee_with_zero_attendance_logs(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", department="Engineering", joined_on="2024-01-01")
    create_sample_emp(client, emp_code="EMP0002", department="Engineering", joined_on="2024-01-01")
    client.post(
        "/attendance/punch-in",
        json={"emp_code": "EMP0001", "punched_at": ist_to_ms(2025, 1, 2, 9, 0, 0)},
    )
    summary = client.get("/analytics/departments/summary", params={"month": "2025-01"}).json()
    eng = summary["items"][0]
    assert eng["department"] == "Engineering"
    assert eng["headcount"] == 2


def test_16_employee_joining_mid_month(client: TestClient):
    create_sample_emp(client, emp_code="EMP0010", joined_on="2025-01-20")
    client.post(
        "/attendance/punch-in",
        json={"emp_code": "EMP0010", "punched_at": ist_to_ms(2025, 1, 20, 9, 0, 0)},
    )
    monthly = client.get(
        "/analytics/employees/EMP0010/monthly", params={"month": "2025-01"}
    ).json()
    assert monthly["working_days"] == 10
    assert monthly["present_days"] == 1.0
    assert monthly["attendance_pct"] == 10.0


def test_17_weekend_attendance(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", joined_on="2024-01-01")
    p_in = ist_to_ms(2025, 1, 4, 9, 25, 0)
    p_out = ist_to_ms(2025, 1, 4, 18, 45, 0)
    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": p_in})
    client.post("/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": p_out})

    monthly = client.get(
        "/analytics/employees/EMP0001/monthly", params={"month": "2025-01"}
    ).json()
    assert monthly["present_days"] == 0.0
    assert monthly["late_count"] == 1
    assert monthly["total_late_minutes"] == 25
    assert monthly["total_overtime_minutes"] == 45


def test_18_and_19_leaderboard_ties_and_cutoff_ties(client: TestClient):
    for code in ("EMP0001", "EMP0002", "EMP0003", "EMP0004"):
        create_sample_emp(client, emp_code=code, department="Engineering")

    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_to_ms(2025, 1, 2, 9, 40)})
    client.post("/attendance/punch-in", json={"emp_code": "EMP0002", "punched_at": ist_to_ms(2025, 1, 2, 9, 25)})
    client.post("/attendance/punch-in", json={"emp_code": "EMP0003", "punched_at": ist_to_ms(2025, 1, 2, 9, 25)})
    client.post("/attendance/punch-in", json={"emp_code": "EMP0004", "punched_at": ist_to_ms(2025, 1, 2, 9, 15)})

    lb = client.get("/analytics/leaderboard/late", params={"month": "2025-01", "limit": 2}).json()
    items = lb["items"]
    assert len(items) == 3
    assert [i["rank"] for i in items] == [1, 2, 2]
    assert [i["emp_code"] for i in items] == ["EMP0001", "EMP0002", "EMP0003"]

    lb_all = client.get("/analytics/leaderboard/late", params={"month": "2025-01", "limit": 10}).json()
    assert [i["rank"] for i in lb_all["items"]] == [1, 2, 2, 4]


def test_20_and_21_department_trend_missing_dates_and_moving_avg(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", department="Engineering", joined_on="2025-01-01")
    create_sample_emp(client, emp_code="EMP0002", department="Engineering", joined_on="2025-01-01")

    client.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist_to_ms(2025, 1, 2, 9, 0)})

    trend = client.get(
        "/analytics/departments/Engineering/trend",
        params={"from": "2025-01-01", "to": "2025-01-06"},
    ).json()
    rows = trend["items"]
    assert len(rows) == 6
    assert rows[0]["date"] == "2025-01-01"
    assert rows[0]["attendance_rate"] == 0.0
    assert rows[0]["moving_avg_7d"] == 0.0
    assert rows[1]["date"] == "2025-01-02"
    assert rows[1]["attendance_rate"] == 0.5
    assert rows[1]["moving_avg_7d"] == 0.25
    assert rows[3]["date"] == "2025-01-04"
    assert rows[3]["is_working_day"] is False
    assert rows[3]["attendance_rate"] is None
    assert rows[3]["moving_avg_7d"] == 0.1667


def test_22_empty_analytics_results(client: TestClient):
    create_sample_emp(client, emp_code="EMP0050", department="Sales", joined_on="2025-03-01")
    m = client.get("/analytics/employees/EMP0050/monthly", params={"month": "2025-01"}).json()
    assert m["working_days"] == 0
    assert m["attendance_pct"] is None

    lb = client.get("/analytics/leaderboard/late", params={"month": "2025-01"}).json()
    assert lb["items"] == []


def test_23_invalid_timestamps(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001")
    for bad_ts in ("1735788600000", 1735788600.5, 1735788600, True, -500):
        resp = client.post(
            "/attendance/punch-in",
            json={"emp_code": "EMP0001", "punched_at": bad_ts},
        )
        assert resp.status_code == 422


def test_24_invalid_date_ranges(client: TestClient):
    create_sample_emp(client, emp_code="EMP0001", department="Engineering")
    r1 = client.get("/attendance", params={"date_from": "2025-01-10", "date_to": "2025-01-01"})
    assert r1.status_code == 422

    r2 = client.get(
        "/analytics/departments/Engineering/trend",
        params={"from": "2025-01-10", "to": "2025-01-01"},
    )
    assert r2.status_code == 422

    r3 = client.get(
        "/analytics/departments/Engineering/trend",
        params={"from": "2025-01-01", "to": "2025-05-01"},
    )
    assert r3.status_code == 422


def test_25_pagination_filtering_and_explain_ixscan(client: TestClient):
    for i in range(1, 6):
        create_sample_emp(
            client,
            emp_code=f"EMP000{i}",
            department="Engineering" if i <= 3 else "Operations",
        )
        client.post(
            "/attendance/punch-in",
            json={"emp_code": f"EMP000{i}", "punched_at": ist_to_ms(2025, 1, 2, 9, 20, 0)},
        )

    emp_page = client.get(
        "/employees", params={"department": "Engineering", "page": 1, "page_size": 2}
    ).json()
    assert emp_page["total"] == 3
    assert len(emp_page["items"]) == 2
    assert "_id" not in emp_page["items"][0]

    for ep in (
        "attendance_list",
        "employee_monthly",
        "department_summary",
        "late_leaderboard",
        "department_trend",
    ):
        ex = client.get(f"/admin/explain/{ep}")
        assert ex.status_code == 200
        plan_str = str(ex.json()["explain"])
        assert "IXSCAN" in plan_str
        assert "COLLSCAN" not in plan_str
