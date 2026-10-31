# Architectural & Database Decisions

### 1. Indexes Created and Rejected
On `employees`, we created unique `{emp_code: 1}`, `{department: 1, joined_on: 1, emp_code: 1}`, and `{joined_on: 1, department: 1}`. On `attendance_logs`, we created unique `{emp_code: 1, date: 1}`, `{date: -1, emp_code: 1, status: 1}`, `{date: 1, late_minutes: 1, emp_code: 1}`, and `{emp_code: 1, punch_in: -1}`. Single-field indexes on `status` and `late_minutes` were rejected for low selectivity.

### 2. Simultaneous Punch-In Requests
Simultaneous requests compute the IST shift `date` and execute `insert_one`. MongoDB's unique compound index `{emp_code: 1, date: 1}` serializes key insertion atomically: one request succeeds (`HTTP 201`), while concurrent duplicates raise `DuplicateKeyError` (`HTTP 409`).

### 3. Leaderboard Ranking with Cutoff Ties
After grouping `late_minutes > 0` by employee and filtering by department, `$setWindowFields` with `$rank` sorted by `total_late_minutes: -1` assigns competition ranks (`1, 2, 2, 4`). Sorting by `total_late_minutes: -1, _id: 1` and matching `rank <= limit` keeps all cutoff ties.

### 4. Department Summary with Zero-Log Employees
The pipeline starts on `employees` (`joined_on <= month_end`), joins monthly `attendance_logs` via `$lookup`, and unwinds with `preserveNullAndEmptyArrays: true` so `$addToSet: "$emp_code"` counts employees without logs.

### 5. Scaling to 100x More Data
Maintain incremental daily/monthly rollup collections via change streams, partition historical `attendance_logs` by date, and route analytics reads to replica secondaries.
