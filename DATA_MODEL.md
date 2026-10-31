# Data Model Specification

## 1. Overview

The Employee Attendance & Analytics API uses MongoDB 6.0+ as its primary persistence and analytics engine. The database consists of two core collections:

- `employees`
- `attendance_logs`

All timestamps representing concrete instants in time are stored in MongoDB as **UTC BSON `Date` (`datetime`)** values truncated to whole seconds, and serialized in HTTP JSON payloads as **integer epoch milliseconds**. Calendar dates (such as `joined_on` and attendance `date`) are stored and serialized as ISO-8601 date strings (`YYYY-MM-DD`) in IST (`UTC+05:30`).

---

## 2. Collection: `employees`

Each document represents an employee profile and their assigned shift window in IST (`UTC+05:30`).

### Document Schema

| Field | MongoDB BSON Type | API JSON Type | Constraints & Description |
| :--- | :--- | :--- | :--- |
| `_id` | `ObjectId` | *(Excluded)* | Internal MongoDB identifier. Never exposed in API responses. |
| `emp_code` | `String` | `string` | Unique employee code matching `^EMP\d{4,6}$` (e.g., `EMP0001`). |
| `name` | `String` | `string` | Non-empty trimmed employee full name (`1..120` chars). |
| `email` | `String` | `string` | Valid email address, normalized to lowercase. |
| `department` | `String` | `string` | Non-empty trimmed department name (e.g., `Engineering`, `Operations`, `Sales`, `HR`). |
| `joined_on` | `String` | `string` | Joining calendar date in `YYYY-MM-DD` format. |
| `shift_start` | `String` | `string` | Shift start time in 24-hour `HH:MM` format (IST). |
| `shift_end` | `String` | `string` | Shift end time in 24-hour `HH:MM` format (IST). Must differ from `shift_start`. Overnight shifts occur when `shift_end <= shift_start`. |
| `created_at` | `Date` (UTC) | `integer` (epoch ms) | Server-generated creation timestamp truncated to whole seconds. |

### Indexes on `employees`

1. **`idx_emp_code_unique`**: `{ "emp_code": 1 }` (`unique: true`)
   - Enforces uniqueness of `emp_code` under concurrent `POST /employees` requests and accelerates employee lookups and `$lookup` joins.
2. **`idx_dept_joined_emp`**: `{ "department": 1, "joined_on": 1, "emp_code": 1 }`
   - Supports filtered department queries (`GET /employees`), department headcount lookups (`joined_on <= date`), and department trend seeding.
3. **`idx_joined_dept`**: `{ "joined_on": 1, "department": 1 }`
   - Supports `GET /analytics/departments/summary` when no department filter is supplied, ensuring an index scan (`IXSCAN`) over `joined_on <= month_end`.

---

## 3. Collection: `attendance_logs`

Each document represents a single employee's attendance record for a single IST shift date.

### Document Schema

| Field | MongoDB BSON Type | API JSON Type | Constraints & Description |
| :--- | :--- | :--- | :--- |
| `_id` | `ObjectId` | *(Excluded)* | Internal MongoDB identifier. Never exposed in API responses. |
| `emp_code` | `String` | `string` | References `employees.emp_code`. |
| `date` | `String` | `string` | Shift attendance date (`YYYY-MM-DD`) in IST, computed via Rule R1. |
| `status` | `String` | `string` | One of `PRESENT`, `WFH`, `ON_DUTY`, `ABSENT`, `LEAVE`. |
| `punch_in` | `Date` (UTC) or `Null` | `integer` (epoch ms) or `null` | Punch-in instant truncated to whole seconds. Required for presence statuses (`PRESENT`, `WFH`, `ON_DUTY`); `null` for `ABSENT` and `LEAVE`. |
| `punch_out` | `Date` (UTC) or `Null` | `integer` (epoch ms) or `null` | Punch-out instant truncated to whole seconds. `null` when shift is still open or for non-presence statuses. |
| `late_minutes` | `Int32` / `Int64` | `integer` | Derived via Rule R2. Whole elapsed minutes after `shift_start` (using `floor`) when arrival is strictly greater than 10 minutes after `shift_start`; otherwise `0`. |
| `work_hours` | `Double` or `Null` | `number` or `null` | Derived via Rule R4. Elapsed hours between `punch_out` and `punch_in` rounded to 2 decimal places using half-up rounding (`ROUND_HALF_UP`). `null` if `punch_out` is `null`. |
| `overtime_minutes` | `Int32` / `Int64` | `integer` | Derived via Rule R3. Whole minutes worked after `shift_end` if `>= 30`; otherwise `0`. |
| `half_day` | `Boolean` | `boolean` | Derived via Rule R5. `true` if rounded `work_hours < 4.50` (when `work_hours` is non-null); otherwise `false`. |
| `history` | `Array<Object>` | `array` | Ordered list of manual regularization audit entries appended exclusively by `PATCH /attendance/{emp_code}/{date}`. Empty array `[]` on initial punch-in/punch-out. |

### Audit History Entry Schema (`history[]`)

| Field | MongoDB BSON Type | API JSON Type | Description |
| :--- | :--- | :--- | :--- |
| `at` | `Date` (UTC) | `integer` (epoch ms) | Server timestamp when the manual correction was applied (truncated to whole seconds). |
| `by` | `String` | `string` | Identifier of the user/admin who regularized the record (`regularized_by`). |
| `reason` | `String` | `string` | Non-empty explanation for the manual regularization. |
| `changes` | `Object` | `object` | Map of only the fields (`status`, `punch_in`, `punch_out`) whose values actually changed, each containing `{"from": <old>, "to": <new>}`. Nested punch timestamps are stored as BSON `Date` (or `null`) and returned as integer epoch ms (or `null`). |

### Indexes on `attendance_logs`

1. **`idx_emp_date_unique`**: `{ "emp_code": 1, "date": 1 }` (`unique: true`)
   - Enforces at most one attendance document per `(emp_code, date)` under concurrent punch-ins, and serves `PATCH /attendance/{emp_code}/{date}` and `GET /analytics/employees/{emp_code}/monthly`.
2. **`idx_date_emp_status`**: `{ "date": -1, "emp_code": 1, "status": 1 }`
   - Supports `GET /attendance` sorting (`date DESC, emp_code ASC`) and date-bounded analytics pipelines (`department_summary`, `late_leaderboard`, `department_trend`) via `IXSCAN`.
3. **`idx_emp_punch_in`**: `{ "emp_code": 1, "punch_in": -1 }`
   - Supports `POST /attendance/punch-out` lookup for the employee's most recent attendance record with `punch_in <= punched_at`.
