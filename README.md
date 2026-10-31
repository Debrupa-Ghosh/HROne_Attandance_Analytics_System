# Employee Attendance & Analytics API

Production backend service implementing employee management, IST shift-aware punch-in/punch-out tracking, atomic manual attendance regularization, MongoDB 6.0+ aggregation analytics pipelines, and `executionStats` query explain inspection.

---

## 1. Technology Stack

- **Language**: Python 3.11+ (compatible with Python 3.10+)
- **Web Framework**: FastAPI + Uvicorn
- **Database**: MongoDB 6.0+ / MongoDB Atlas
- **Driver**: PyMongo 4.7+ (`pymongo[srv]`, synchronous driver with timezone-aware UTC BSON handling)
- **Validation**: Pydantic v2 + `email-validator`
- **Configuration**: `python-dotenv`
- **Testing**: `pytest` + `httpx`

---

## 2. MongoDB Atlas & Environment Variable Configuration

The application reads database settings from environment variables (which take priority over any `.env` file). `.env` is excluded from Git via `.gitignore`.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `MONGO_URI` | MongoDB Atlas SRV or standard connection string | `mongodb://localhost:27017` |
| `MONGO_DB` | Target MongoDB database for the application | `attendance_db` |
| `MONGO_TEST_DB` | Isolated MongoDB database used exclusively by `pytest` | `attendance_test_db` |

Example configuration:

```bash
export MONGO_URI="mongodb+srv://<username>:<password>@<cluster>.mongodb.net/"
export MONGO_DB="attendance_db"
```

---

## 3. Dependency Installation

Create a virtual environment and install dependencies from `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 4. Manual Sample Data Seeding & Application Startup

FastAPI startup **never** automatically seeds, overwrites, or deletes documents (preserving existing and evaluator datasets). It only creates required indexes idempotently.

To populate local development sample data (`sample_data/employees.json` and `sample_data/attendance_logs.json`) into `attendance_db`, run `sample_seed.py` manually:

```bash
python3 sample_seed.py
```

Start the FastAPI server using Uvicorn on port `8000`:

```bash
uvicorn app.main:app --port 8000
```

---

## 5. Interactive API Documentation

Once the server is running on port `8000`, interactive OpenAPI documentation is available at:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

---

## 6. Running Automated Tests (`attendance_test_db`)

The test suite (`tests/test_api.py`) connects strictly to the isolated `attendance_test_db` database on the configured MongoDB cluster so application and evaluator data in `attendance_db` are never modified or deleted:

```bash
pytest -v
```
