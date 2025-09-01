# Mega-Infrastructure EVM Master Control System

# Mega-Infrastructure EVM Master Control System

[![Build](https://github.com/James1979/mega-evm-master-control/actions/workflows/ci.yml/badge.svg)](https://github.com/James1979/mega-evm-master-control/actions)
[![codecov](https://codecov.io/gh/James1979/mega-evm-master-control/branch/main/graph/badge.svg)](https://codecov.io/gh/James1979/mega-evm-master-control)

[![Build](https://img.shields.io/github/actions/workflow/status/James1979/mega-evm-master-control/ci.yml)](./.github/workflows/ci.yml)
![Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)

**Enterprise-grade EVM + Risk + AI Early Warning and Forecasting for mega-infrastructure (PG&E/Bechtel class).**

This system integrates **cost, schedule, and risk data** to provide real-time **Earned Value Management (EVM)** KPIs, **Monte Carlo simulations** (P10/P50/P90), and **AI-powered variance narratives**.  
Built for **utilities, infrastructure, and mega-projects**, this project demonstrates enterprise-level quality, security, and DevOps practices.

---

## 🔥 Demo
- **20-sec GIF:** `docs/demo.gif` (to be added)  
- **Screenshots:** `docs/img/` (dashboard, risk heatmap, P-curve)

---

## 🧭 Architecture

```mermaid
flowchart LR
  A[Source Systems: Cost, Schedule, Procurement] --> B[ETL (/etl)]
  B --> C[Processed Data (/data/processed)]
  C --> D[EVM Core (CPI/SPI/EAC)]
  D --> E[Monte Carlo (P10/P50/P90)]
  E --> F[AI Variance Narratives]
  D --> G[Dashboards (Streamlit)]
  E --> G
  F --> G
  G --> H[Alerts (Email/Slack)]


## 🚀 Quick Start (Run Locally)

```bash
# 1. Create and activate a virtual environment
python -m venv .venv          # Create a virtual environment in a folder named .venv
.\.venv\Scripts\activate      # Windows: Activate the virtual environment
# source .venv/bin/activate   # Linux/Mac: Activate the virtual environment

# 2. Upgrade pip and install all dependencies
pip install -U pip            # Upgrade pip to the latest version
pip install -r requirements.txt  # Install all required Python packages

# 3. Start the Streamlit app
bash scripts/run_app.sh       # Linux/Mac: Run the app via shell script
# OR for Windows:
scripts\run_app.bat           # Run the app via batch script

# The dashboard will now be available at: http://localhost:8501 

```