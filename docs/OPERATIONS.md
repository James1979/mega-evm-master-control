# OPERATIONS

## Environments
- `ENV=dev|prod`

## Common Issues
- **Port 8501 in use**: close other Streamlit instances or set `--server.port 8502`.
- **Windows venv activation**: run PowerShell as Admin; `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

## SLOs (suggested)
- Alerts evaluated every 15 minutes.
- Dashboard loads < 3s on sample data.

## Logs
- Place app logs under `logs/` (e.g., `logs/alerts_audit.csv`).

## Upgrades
- Bump deps, run `pip-audit`, regenerate `sbom.json`.
