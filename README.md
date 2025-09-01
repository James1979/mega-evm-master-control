# Mega-Infrastructure EVM Master Control System
**Built by James Lim @ ITC Management Group**

![ITC](assets/itcmanagement_group.jpg)

## Quickstart (Windows PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate
pip install --upgrade pip
pip install -r requirements.txt
copy .env.example .env
.\scripts\run_etl.bat
python -m services.ai_variance_narratives --project_id DEMO
python -m services.alerts
.\scripts\run_app.bat
```
Open http://localhost:8501
