## Quick Start (5 minutes)

**Option A — Docker (recommended)**
```bash
# from repo root
docker compose up --build
# open http://localhost:8501
```

**Option B — Local (Windows PowerShell)**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt
streamlit run streamlit_app/app.py
```

**Run tests**
```bash
make test
# or
pytest -q --cov=./ --cov-report=term-missing --cov-fail-under=80
```
