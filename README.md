<!--
TITLE + BADGES
- Keep the title unique (no duplicates).
- Badges show CI status and live coverage pulled from Codecov/Shields.
-->

# Mega-Infrastructure EVM Master Control System

<!-- GitHub Actions (CI) badge: shows whether the latest workflow run passed -->
[![Build](https://github.com/James1979/mega-evm-master-control/actions/workflows/ci.yml/badge.svg)](https://github.com/James1979/mega-evm-master-control/actions)

<!-- Codecov native badge: links to detailed coverage reports on codecov.io -->
[![codecov](https://codecov.io/gh/James1979/mega-evm-master-control/branch/main/graph/badge.svg)](https://codecov.io/gh/James1979/mega-evm-master-control)

<!-- Shields.io coverage badge (auto-colored): quick visual thresholding -->
[![coverage](https://img.shields.io/codecov/c/github/James1979/mega-evm-master-control?branch=main&label=coverage&logo=codecov)](https://codecov.io/gh/James1979/mega-evm-master-control)

<!-- Misc project info badges -->
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)

<!--
VALUE PROP
- Concise statement of what this project does and for whom.
-->
**Enterprise-grade EVM + Risk + AI Early Warning and Forecasting for mega-infrastructure (PG&E/Bechtel class).**

This system integrates **cost, schedule, and risk data** to provide real-time **Earned Value Management (EVM)** KPIs, **Monte Carlo simulations** (P10/P50/P90), and **AI-powered variance narratives**.  
Built for **utilities, infrastructure, and mega-projects**, this project demonstrates enterprise-level quality, security, and DevOps practices.

---

<!--
QUALITY GATES
- At-a-glance table that hiring managers love.
- Links/badges reflect your existing CI configuration.
-->
## 🏆 Quality Gates

| Check                     | Status                                                                                                   | Description                                              |
|---------------------------|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| **Linting**               | ✅ [![ruff](https://img.shields.io/badge/ruff-passing-brightgreen?logo=python&logoColor=white)](#)        | Ensures code style and formatting using `ruff`.          |
| **Type Checking**         | ✅ [![mypy](https://img.shields.io/badge/mypy-checked-blue?logo=python&logoColor=white)](#)              | Static type checks for Python with `mypy`.               |
| **Unit Tests**            | ✅ [![pytest](https://img.shields.io/badge/tests-passing-brightgreen?logo=pytest)](#)                   | Full test suite with `pytest`.                           |
| **Coverage**              | [![coverage](https://img.shields.io/codecov/c/github/James1979/mega-evm-master-control?branch=main&label=coverage&logo=codecov)](https://codecov.io/gh/James1979/mega-evm-master-control) | Coverage enforced in CI with `--cov-fail-under=80`.      |
| **Continuous Integration**| ✅ [![Build](https://github.com/James1979/mega-evm-master-control/actions/workflows/ci.yml/badge.svg)](https://github.com/James1979/mega-evm-master-control/actions) | Automated QA via GitHub Actions.                         |

---

<!--
DEMO PLACEHOLDERS
- Keeps recruiters oriented; they can open docs/img or preview a GIF later.
-->
## 🔥 Demo
- **20-sec GIF:** `docs/demo.gif` *(placeholder; add your GIF to this path)*  
- **Screenshots:** `docs/img/` *(dashboard, risk heatmap, P-curve)*

---

<!--
ARCHITECTURE DIAGRAM (Mermaid)
- FIX: Close the code fence properly. Do not mix bash code within mermaid block.
-->
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
