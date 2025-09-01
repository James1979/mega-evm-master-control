# Mega-Infrastructure EVM Master Control System

[![Build](https://github.com/James1979/mega-evm-master-control/actions/workflows/ci.yml/badge.svg)](https://github.com/James1979/mega-evm-master-control/actions)
[![codecov](https://codecov.io/gh/James1979/mega-evm-master-control/branch/main/graph/badge.svg)](https://codecov.io/gh/James1979/mega-evm-master-control)
[![coverage](https://img.shields.io/codecov/c/github/James1979/mega-evm-master-control?branch=main&label=coverage&logo=codecov)](https://codecov.io/gh/James1979/mega-evm-master-control)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)

**Enterprise-grade EVM + Risk + AI Early Warning and Forecasting for mega-infrastructure (PG&E/Bechtel class).**

This system integrates **cost, schedule, and risk data** to provide real-time **Earned Value Management (EVM)** KPIs, **Monte Carlo simulations** (P10/P50/P90), and **AI-powered variance narratives**.  
Built for **utilities, infrastructure, and mega-projects**, this project demonstrates enterprise-level quality, security, and DevOps practices.

---

## 🏆 Quality Gates

| Check                     | Status                                                                                                   | Description                                              |
|---------------------------|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| **Linting**               | ✅ [![ruff](https://img.shields.io/badge/ruff-passing-brightgreen?logo=python&logoColor=white)](#)        | Code style and formatting enforced with `ruff`.          |
| **Type Checking**         | ✅ [![mypy](https://img.shields.io/badge/mypy-checked-blue?logo=python&logoColor=white)](#)              | Static type checks for Python using `mypy`.              |
| **Unit Tests**            | ✅ [![pytest](https://img.shields.io/badge/tests-passing-brightgreen?logo=pytest)](#)                   | Full test suite using `pytest`.                          |
| **Coverage**              | [![coverage](https://img.shields.io/codecov/c/github/James1979/mega-evm-master-control?branch=main&label=coverage&logo=codecov)](https://codecov.io/gh/James1979/mega-evm-master-control) | Coverage enforced in CI with `--cov-fail-under=80`.      |
| **Continuous Integration**| ✅ [![Build](https://github.com/James1979/mega-evm-master-control/actions/workflows/ci.yml/badge.svg)](https://github.com/James1979/mega-evm-master-control/actions) | Automated QA via GitHub Actions.                         |

---

## 🔥 Demo
- **20-sec GIF:** `docs/demo.gif` *(placeholder; add your GIF to this path)*  
- **Screenshots:** `docs/img/` *(dashboard, risk heatmap, P-curve)*

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

```

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
---

## 📂 Project Structure

```text
mega-evm-master-control/
├── .github/workflows/           # CI/CD configuration (linting, tests, coverage)
├── data/
│   ├── processed/               # Generated parquet outputs (EVM, Monte Carlo)
│   ├── raw/                     # Raw source files (input data)
│   └── samples/                 # Sample CSV/Parquet data for demos
├── docs/                        # Documentation, diagrams, screenshots
├── etl/                         # ETL pipelines and core calculation scripts
│   ├── evm_calculator.py        # Earned Value Management metrics engine
│   ├── monte_carlo.py           # Monte Carlo simulation engine
│   ├── p6_ingest.py             # Primavera P6 ingestion pipeline
│   └── procurement_join.py      # Procurement and cost impact joins
├── services/                    # Microservices for AI, alerts, integrations
│   ├── ai_variance_narratives.py# AI-driven variance narrative generation
│   └── alerts.py                # Email/Slack/Jira alerting service
├── streamlit_app/               # Streamlit UI frontend
│   └── app.py                   # Dashboard app entrypoint
├── tests/                       # Full pytest suite (unit, smoke, CLI tests)
├── scripts/                     # Helper scripts for local dev & CI
├── requirements.txt             # Python dependencies
├── pytest.ini                   # Pytest config (warnings, verbosity)
├── .coveragerc                  # Coverage configuration (ignore UI/tests)
└── README.md                    # This file

---

## ✨ Features

- **📊 Earned Value Management (EVM) Engine**
  - Computes CPI, SPI, VAC, CV, SV, and EAC in real time
  - Integrates Primavera P6, cost, and procurement data

- **🎲 Monte Carlo Forecasting**
  - P10 / P50 / P90 confidence intervals for cost & schedule
  - Generates S-curve datasets for Power BI or Streamlit dashboards

- **🤖 AI-Powered Variance Narratives**
  - Automatically explains cost & schedule variances
  - Ready for Slack, Jira, and email alerts

- **📈 Enterprise-Ready Dashboards**
  - Streamlit-based web app for executives & PM teams
  - Risk heatmaps, performance trends, and early warning triggers

- **🔗 Seamless Integrations**
  - Hooks for Jira, Slack, and Power BI
  - CI/CD via GitHub Actions, Codecov coverage enforcement

- **✅ DevOps & Quality Gates**
  - 80%+ test coverage, type-checking (mypy), linting (ruff)
  - Full GitHub Actions pipeline with coverage badges
  - `.coveragerc` and `pytest.ini` for professional workflows

---
