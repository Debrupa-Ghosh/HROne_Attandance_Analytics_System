# Remove Duplicate `/candidate_kit` Directory

## 1. Safety Verification
Deleting `/candidate_kit` will cause **zero problems**:
- **FastAPI (`app/main.py`)**: Runs from `/app/main.py` at the repository root.
- **Database Seeder (`sample_seed.py`)**: Reads `/sample_data/employees.json` and `/sample_data/attendance_logs.json` from the repository root.
- **Automated Tests (`tests/test_api.py`)**: Imports `from app.main import app` at the repository root.
- **Live Preview Workbench (`server.ts`)**: Reads all deliverable files (`app/main.py`, `openapi.yaml`, `DATA_MODEL.md`, `REVIEW.md`, `DECISIONS.md`, `README.md`, `requirements.txt`, `sample_seed.py`, `tests/test_api.py`, and `sample_data/*.json`) directly from the repository root.

## 2. Action (Optional — Click Proceed to Execute)
- Permanently delete `/candidate_kit` from the workspace using `delete_dir` (`Force: true`) so the duplicate folder is completely removed from your project tree.
