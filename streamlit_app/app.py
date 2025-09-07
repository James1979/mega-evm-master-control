"""
Mega-EVM Master Control – What-If Copilot
Executive Streamlit UI (demo-safe with dry-run), built to be easy to explain.

WHAT THIS APP DOES
------------------
• Reads settings from config.yaml (paths, logo, thresholds, Monte Carlo defaults, alerts dry-run).
• Lets a hiring manager upload sample CSVs into data/samples/.
• Runs the ETL pipeline (scripts/run_etl.bat) and Monte Carlo engine (etl.monte_carlo).
• Shows portfolio KPIs, Forecast (P50/P80 + S-curve), Procurement impacts, Alert history.
• AI Copilot: generates proactive recommendations (cloud LLM if keys exist, otherwise rule-based).
• One-click “Create Ticket(s)” button calls services.alerts (honors dry-run so it’s safe to demo).
• Governance panel: flip Dry-Run ON/OFF and enable/disable JIRA/Slack/Email, writing back to config.yaml.

HOW TO TEACH THIS
-----------------
1) “Sidebar controls let you load data, run pipelines, and manage governance (dry-run).”
2) “Tabs separate KPIs, forecast risk, procurement, alerts, and AI guidance.”
3) “Everything reads/writes simple files, so it’s auditable and enterprise-friendly.”
"""

from __future__ import annotations

# ── Standard library
import json
import os
import platform
import subprocess  # nosec B404 - used via safe wrapper with shell=False
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

# ── Third-party
import pandas as pd
import plotly.express as px
import streamlit as st
import yaml

# Pillow is optional; only used for rendering a logo if present
try:
    from PIL import Image  # noqa: F401
except Exception:  # pragma: no cover - optional dependency
    Image = None  # mypy-safe: keep a single name, type may be None


# ──────────────────────────────────────────────────────────────────────────────
# 1) CONFIG LOADING — merge config.yaml with defaults so missing keys never crash
# ──────────────────────────────────────────────────────────────────────────────
def load_config(cfg_path: Path) -> Dict[str, Any]:
    """Return a config dict: defaults overlaid with user config.yaml if present."""
    defaults: Dict[str, Any] = {
        "thresholds": {
            "cpi_red": 0.90,
            "spi_red": 0.85,
            "vac_negative": 0.0,
            "p80_variance_cost_warn": 0.05,
            "p80_variance_days_warn": 10,
        },
        "paths": {
            "samples_dir": "data/samples",
            "processed_dir": "data/processed",
            "charts_dir": "charts",
        },
        "monte_carlo": {
            "iterations": 5000,
            "seed": 42,
            "distribution": "PERT",
            "default_procurement_delay_days": [0, 5, 15],
            "sensitivity": ["spearman", "pearson"],
        },
        "ui": {"logo_path": "assets/itcmanagement_group.jpg", "theme": "dark"},
        "scenario": {
            "delay_cost_rate_per_day": 15000,
            "spi_productivity_sensitivity": 0.5,
            "cpi_resource_sensitivity": 0.4,
        },
        "ai": {"provider": "env:LLM_PROVIDER", "model": "env:LLM_MODEL", "temperature": 0.2, "max_tokens": 1200},
        "alerts": {"slack_enabled": True, "email_enabled": True, "jira_enabled": True, "dry_run": True},
    }

    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as f:
            user_cfg = yaml.safe_load(f) or {}
        for k, v in user_cfg.items():
            if isinstance(v, dict) and k in defaults and isinstance(defaults[k], dict):
                defaults[k].update(v)  # shallow-merge dicts
            else:
                defaults[k] = v
    return defaults


# ──────────────────────────────────────────────────────────────────────────────
# 2) PATHS & PAGE SETUP — constants and page config (icon is a string path)
# ──────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "config.yaml"
CFG = load_config(CFG_PATH)

SAMPLES_DIR = ROOT / CFG["paths"]["samples_dir"]
PROCESSED_DIR = ROOT / CFG["paths"]["processed_dir"]
CHARTS_DIR = ROOT / CFG["paths"]["charts_dir"]

EVM_FP = PROCESSED_DIR / "evm_timeseries.parquet"
MC_SUM_FP = PROCESSED_DIR / "monte_carlo_summary.parquet"
PROC_FP = PROCESSED_DIR / "procurement_impacts.parquet"
RUNS_FP = PROCESSED_DIR / "monte_carlo_runs.parquet"
SCURVE_FP = PROCESSED_DIR / "forecast_s_curves.parquet"
ALERTS_FP = PROCESSED_DIR / "alerts_outbox.json"
AI_NOTE_FP = PROCESSED_DIR / "ai_recommendations.txt"

# Keep a single stable type (str) for the page icon to satisfy mypy
logo_path = ROOT / CFG["ui"]["logo_path"]
page_icon_for_config: str = str(logo_path) if logo_path.exists() else "📊"

st.set_page_config(
    page_title="Mega-EVM Master Control – What-If Copilot",
    page_icon=page_icon_for_config,  # str path or emoji
    layout="wide",
)

# Basic CSS for a clean executive look
st.markdown(
    """
<style>
.block-container { padding-top: 1rem; padding-bottom: 2rem; }
.hero { background: linear-gradient(90deg, #0B1F3A 0%, #12345c 100%); border-radius: 14px;
        padding: 18px 22px; color: #fff; margin-bottom: 10px; box-shadow: 0 6px 16px rgba(0,0,0,0.2); }
.hero h1 { font-size: 1.5rem; margin: 0; line-height: 1.2; }
.hero p  { margin: 0.2rem 0 0; opacity: 0.9; }
.kpi-card { border: 1px solid #e8e8e8; border-radius: 12px; padding: 12px 14px; background: #fff;
           box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
.kpi-label { font-size: 0.9rem; color: #666; }
.kpi-value { font-weight: 700; font-size: 1.2rem; color: #0B1F3A; }
.small-caption { font-size: 0.85rem; color: #cfd8e3; }
.badge { display:inline-block; padding: 2px 8px; border-radius: 10px; font-size: 0.75rem; }
.badge-green { background:#d1fae5; color:#065f46; }
.badge-red { background:#fee2e2; color:#991b1b; }
</style>
""",
    unsafe_allow_html=True,
)

# Header with logo + product tag
with st.container():
    st.markdown('<div class="hero">', unsafe_allow_html=True)
    cols = st.columns([1, 7], vertical_alignment="center")
    with cols[0]:
        if logo_path.exists():
            st.image(str(logo_path), use_container_width=False)
    with cols[1]:
        st.markdown(
            """
<h1>Mega-EVM Master Control – What-If Copilot</h1>
<p class="small-caption">Built by James Lim @ ITC Management Group</p>
""",
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# 3) HELPERS — file I/O, subprocess, config persistence
# ──────────────────────────────────────────────────────────────────────────────
def load_parquet(fp: Path) -> Optional[pd.DataFrame]:
    """Safely read a parquet file; return None if missing/unreadable."""
    try:
        return pd.read_parquet(fp) if fp.exists() else None
    except Exception as e:
        st.warning(f"Could not read {fp.name}: {e}")
        return None


def load_text(fp: Path) -> Optional[str]:
    """Read a text/JSON file into a string; return None if missing or unreadable."""
    if not fp.exists():
        return None
    try:
        return fp.read_text(encoding="utf-8")
    except Exception as e:
        st.warning(f"Could not read {fp.name}: {e}")
        return None


def run_command(cmd: List[str] | str, cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    """
    Run a command safely with shell=False across platforms.
    Returns (exit_code, stdout, stderr).
    """
    is_windows = platform.system() == "Windows"

    # Normalize the input into a list[str]
    if isinstance(cmd, str):
        cmd_list: List[str] = [cmd]
    else:
        cmd_list = [str(x) for x in cmd]

    # If first token is a .bat/.cmd on Windows, wrap with cmd.exe /c (no shell=True)
    if is_windows and cmd_list:
        first = cmd_list[0].lower()
        if first.endswith(".bat") or first.endswith(".cmd"):
            cmd_list = ["cmd.exe", "/c"] + cmd_list

    proc = subprocess.run(  # nosec B603 - command is constructed, shell=False
        cmd_list,
        cwd=str(cwd) if cwd else None,
        shell=False,  # ✅ never enable shell=True
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def open_file_cross_platform(path: Path) -> Tuple[int, str, str]:
    """Open a file with the OS default app (e.g., .pbix → Power BI) in a safe way."""
    try:
        if platform.system() == "Windows":
            comspec = os.environ.get("COMSPEC") or r"C:\Windows\System32\cmd.exe"
            p = path.resolve()
            if not p.exists():
                return 1, "", f"Not found: {p}"
            if not p.is_file():
                return 1, "", f"Not a file: {p}"
            result = subprocess.run(  # nosec B603 - fixed command, validated local path
                [comspec, "/c", "start", "", str(p)],
                cwd=str(p.parent),
                shell=False,
                capture_output=True,
                text=True,
            )
            return result.returncode, "Opened via cmd start", result.stderr

        if platform.system() == "Darwin":
            return run_command(["open", str(path)])

        return run_command(["xdg-open", str(path)])
    except Exception as e:
        return 1, "", str(e)


def save_config(cfg: Dict[str, Any], path: Path) -> None:
    """Persist the in-memory config back to config.yaml."""
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False, indent=2)


def set_cfg(path_keys: List[str], value: Any, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Set nested config values (e.g., set_cfg(['alerts','dry_run'], True, CFG)).
    Creates intermediate dicts as needed.
    """
    node: Dict[str, Any] = cfg
    for k in path_keys[:-1]:
        if k not in node or not isinstance(node[k], dict):
            node[k] = {}
        node = cast(Dict[str, Any], node[k])
    node[path_keys[-1]] = value
    return cfg


def _env_nonempty(var_name: str) -> bool:
    """True if env var exists and is not just whitespace."""
    val = os.getenv(var_name, "")
    return bool(val and val.strip())


# ──────────────────────────────────────────────────────────────────────────────
# 4) SIDEBAR — Upload → ETL/MonteCarlo → PowerBI → Governance (writes config)
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Controls")

    # Upload CSVs into data/samples for ETL ingestion
    st.subheader("Upload your data (CSV)")
    st.write("Use these **exact filenames** so ETL will pick them up:")
    st.markdown("- `schedule_activities.csv`\n- `cost_erp.csv`\n- `procurement.csv`\n- `risk_register.csv`")
    uploaded = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True)
    if uploaded:
        SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
        allowed = {
            "schedule_activities.csv",
            "cost_erp.csv",
            "procurement.csv",
            "risk_register.csv",
        }
        saved: List[str] = []
        for uf in uploaded:
            if uf.name.lower() in allowed:
                (SAMPLES_DIR / uf.name).write_bytes(uf.getvalue())
                saved.append(uf.name)
        if saved:
            st.success(f"Saved to `{CFG['paths']['samples_dir']}/`: {', '.join(saved)}")
        else:
            st.warning("No standard filenames detected. Rename to the list above so ETL will read them.")

    st.divider()

    # Run full ETL (.bat) – parses CSVs → EVM → Procurement → Monte Carlo
    st.subheader("Run ETL (full pipeline)")
    st.caption("Parses CSVs → computes EVM → joins procurement → runs Monte Carlo.")
    if st.button("▶ Run ETL Now", use_container_width=True):
        etl_bat = ROOT / "scripts" / "run_etl.bat"
        if not etl_bat.exists():
            st.error(f"Missing script: {etl_bat}")
        else:
            with st.spinner("Running ETL…"):
                code, out, err = run_command([str(etl_bat)], cwd=ROOT)
            if code == 0:
                st.success("ETL completed. Press Ctrl+F5 to refresh charts.")
            else:
                st.error("ETL failed. See logs below.")
            with st.expander("Show ETL output"):
                st.code(out or "<no stdout>")
                if err:
                    st.code(err)

    # Monte Carlo only – handy for what-if without full ETL
    st.divider()
    st.subheader("Run Monte Carlo only")
    iters = st.slider(
        "Iterations", min_value=1_000, max_value=50_000, value=int(CFG["monte_carlo"]["iterations"]), step=1_000
    )
    seed = st.number_input("Random seed", min_value=0, value=int(CFG["monte_carlo"]["seed"]), step=1)
    if st.button("🎲 Run Monte Carlo", use_container_width=True):
        with st.spinner("Running Monte Carlo…"):
            python = sys.executable
            cmd = [
                python,
                "-m",
                "etl.monte_carlo",
                "--iters",
                str(iters),
                "--seed",
                str(seed),
                "--processed",
                str(PROCESSED_DIR),
                "--samples",
                str(SAMPLES_DIR),
                "--outdir",
                str(PROCESSED_DIR),
            ]
            code, out, err = run_command(cmd, cwd=ROOT)
        if code == 0:
            st.success("Monte Carlo completed. Press Ctrl+F5 to refresh charts.")
        else:
            st.error("Monte Carlo failed. See logs below.")
        with st.expander("Show Monte Carlo output"):
            st.code(out or "<no stdout>")
            if err:
                st.code(err)

    # Open Power BI report (if present)
    st.divider()
    st.subheader("Open Power BI")
    st.caption("Launches the executive dashboard pack (.pbix).")
    if st.button("📂 Open MasterControl.pbix", use_container_width=True):
        pbix = ROOT / "dashboards" / "PowerBI" / "MasterControl.pbix"
        if not pbix.exists():
            st.error(f"File not found: {pbix}")
        else:
            code, out, err = open_file_cross_platform(pbix)
            if code == 0:
                st.success("Opening Power BI…")
            else:
                st.error("Could not open Power BI file. See message below.")
                st.code(err or out or "Unknown error")

    # Status and Governance (writes config.yaml)
    st.divider()
    st.subheader("Environment & Alerts Status")

    dry_run_current = bool(CFG["alerts"].get("dry_run", True))
    on_html = '<span class="badge badge-green">ON</span>'
    off_html = '<span class="badge badge-red">OFF</span>'
    status_html = on_html if dry_run_current else off_html
    st.markdown(f"Dry-Run: {status_html}", unsafe_allow_html=True)

    # Quick env checks (non-empty)
    slack_ok = _env_nonempty("SLACK_WEBHOOK_URL")
    email_ok = all(_env_nonempty(v) for v in ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "ALERT_TO"])
    jira_ok = all(_env_nonempty(v) for v in ["JIRA_API_URL", "JIRA_USER_EMAIL", "JIRA_API_TOKEN", "JIRA_PROJECT_KEY"])

    slack_enabled_current = bool(CFG["alerts"].get("slack_enabled", True))
    email_enabled_current = bool(CFG["alerts"].get("email_enabled", True))
    jira_enabled_current = bool(CFG["alerts"].get("jira_enabled", True))

    st.write(f"Slack Webhook: {'✅' if slack_ok else '⚪'} (enabled: {slack_enabled_current})")
    st.write(f"Email (SMTP): {'✅' if email_ok else '⚪'} (enabled: {email_enabled_current})")
    st.write(f"JIRA Cloud: {'✅' if jira_ok else '⚪'} (enabled: {jira_enabled_current})")

    st.caption("Tip: Terminal alternative")
    st.code(".\\scripts\\run_etl.bat", language="powershell")

    # Governance toggles → persist to config.yaml then reload
    st.divider()
    st.subheader("Governance (writes config.yaml)")
    dry_run_toggle = st.toggle(
        "Dry-Run (simulate sends)",
        value=dry_run_current,
        help="ON = safe demo. OFF = real Slack/Email/JIRA (if creds exist).",
    )
    jira_toggle = st.toggle("Enable JIRA channel", value=jira_enabled_current)
    slack_toggle = st.toggle("Enable Slack channel", value=slack_enabled_current)
    email_toggle = st.toggle("Enable Email channel", value=email_enabled_current)

    if st.button("💾 Save & Apply Governance", use_container_width=True, type="primary"):
        set_cfg(["alerts", "dry_run"], bool(dry_run_toggle), CFG)
        set_cfg(["alerts", "jira_enabled"], bool(jira_toggle), CFG)
        set_cfg(["alerts", "slack_enabled"], bool(slack_toggle), CFG)
        set_cfg(["alerts", "email_enabled"], bool(email_toggle), CFG)
        save_config(CFG, CFG_PATH)
        st.success("Saved to config.yaml. Reloading app to apply…")
        st.rerun()  # modern Streamlit reload


# ──────────────────────────────────────────────────────────────────────────────
# 5) LOAD DATA FOR TABS — fail fast if missing + type narrowing for mypy
# ──────────────────────────────────────────────────────────────────────────────
evm_df = load_parquet(EVM_FP)
mc_df = load_parquet(MC_SUM_FP)
proc_df = load_parquet(PROC_FP)
runs_df = load_parquet(RUNS_FP)
scurve_df = load_parquet(SCURVE_FP)
alerts_text = load_text(ALERTS_FP)

with st.expander("Loaded data status", expanded=False):
    st.write(
        {
            "evm_timeseries.parquet": 0 if evm_df is None else len(evm_df),
            "monte_carlo_summary.parquet": 0 if mc_df is None else len(mc_df),
            "procurement_impacts.parquet": 0 if proc_df is None else len(proc_df),
            "monte_carlo_runs.parquet": 0 if runs_df is None else len(runs_df),
            "forecast_s_curves.parquet": 0 if scurve_df is None else len(scurve_df),
            "alerts_outbox.json": 0 if alerts_text is None else len(alerts_text),
        }
    )

if any(x is None for x in (evm_df, mc_df, proc_df)):
    st.warning("Processed files not found. Upload CSVs (optional) and run ETL first.")
    st.stop()

# After st.stop() above, mypy still considers these Optional. Narrow now:
evm_df = cast(pd.DataFrame, evm_df)
mc_df = cast(pd.DataFrame, mc_df)
proc_df = cast(pd.DataFrame, proc_df)


# ──────────────────────────────────────────────────────────────────────────────
# 6) AI UTILITIES — rule-based fallback + optional LLM
# ──────────────────────────────────────────────────────────────────────────────
def build_portfolio_summary(
    evm: pd.DataFrame, mc: pd.DataFrame, proc: pd.DataFrame, projects: Optional[List[Any]] = None
) -> Dict[str, Any]:
    """
    Convert detailed tables into compact JSON for AI:
    - Latest CPI/SPI/EAC/VAC by WBS → portfolio stats
    - Monte Carlo P50/P80 per project
    - Top late procurement items
    """
    # Optional project filter
    if projects:
        evm_f = evm[evm["ProjectID"].isin(projects)].copy()
        mc_f = mc[mc["ProjectID"].isin(projects)].copy()
        proc_f = proc[proc["ProjectID"].isin(projects)] if "ProjectID" in proc.columns else proc.copy()
    else:
        evm_f, mc_f, proc_f = evm.copy(), mc.copy(), proc.copy()

    # Latest row per WBS by Period
    latest = evm_f.copy()
    if "Period" in latest.columns:
        latest["Period"] = pd.to_datetime(latest["Period"], errors="coerce")
        latest = latest.sort_values("Period").groupby(["ProjectID", "WBS"], as_index=False).tail(1)

    cpi_mean = float(latest["CPI"].mean()) if "CPI" in latest.columns and len(latest) else None
    spi_mean = float(latest["SPI"].mean()) if "SPI" in latest.columns and len(latest) else None
    eac_total = float(latest["EAC"].sum()) if "EAC" in latest.columns and len(latest) else None
    vac_total = float(latest["VAC"].sum()) if "VAC" in latest.columns and len(latest) else None

    # Red flags by thresholds — mypy-safe (Series.sum only when column exists)
    cpi_thr = float(CFG["thresholds"]["cpi_red"])
    spi_thr = float(CFG["thresholds"]["spi_red"])
    red_cpi_count = int((latest["CPI"] < cpi_thr).sum()) if "CPI" in latest.columns else 0
    red_spi_count = int((latest["SPI"] < spi_thr).sum()) if "SPI" in latest.columns else 0

    # Monte Carlo records summarized
    mc_records: List[Dict[str, float | Any]] = []
    for _, r in mc_f.iterrows():
        mc_records.append(
            {
                "ProjectID": r.get("ProjectID"),
                "EAC_P50": float(r.get("EAC_P50", 0.0)),
                "EAC_P80": float(r.get("EAC_P80", 0.0)),
                "Finish_P50": float(r.get("Finish_P50", 0.0)),
                "Finish_P80": float(r.get("Finish_P80", 0.0)),
            }
        )

    # Top procurement delays — cast to precise type for mypy
    worst_list: List[Dict[str, Any]] = []
    if "DelayDays" in proc_f.columns and len(proc_f):
        worst_list = cast(
            List[Dict[str, Any]],
            proc_f.sort_values("DelayDays", ascending=False).head(10).to_dict(orient="records"),
        )

    return {
        "portfolio": {
            "cpi_mean": cpi_mean,
            "spi_mean": spi_mean,
            "eac_total": eac_total,
            "vac_total": vac_total,
            "red_cpi_count": red_cpi_count,
            "red_spi_count": red_spi_count,
            "cpi_threshold": cpi_thr,
            "spi_threshold": spi_thr,
        },
        "monte_carlo": mc_records,
        "procurement_top_delays": worst_list,
    }


def rule_based_recs(summary: Dict[str, Any]) -> str:
    """Deterministic guidance that works offline and is great for demos."""
    port = summary.get("portfolio", {})
    cpi = port.get("cpi_mean") or 1.0
    spi = port.get("spi_mean") or 1.0
    cpi_thr = port.get("cpi_threshold", 0.90)
    spi_thr = port.get("spi_threshold", 0.85)
    red_cpi = port.get("red_cpi_count", 0)
    red_spi = port.get("red_spi_count", 0)

    lines: List[str] = []
    lines.append("**AI Copilot — Proactive Prevention Plan**")
    lines.append("1) **Cost Control**")
    if cpi < cpi_thr or red_cpi > 0:
        lines.append(
            f"   • CPI < {cpi_thr:.2f}. Freeze non-critical scope on red WBS; run bottom-up ETC; use EAC=AC+ETC on reds."
        )
        lines.append("   • Tighten change control & value engineering; stage-gate POs > $250k.")
    else:
        lines.append("   • CPI within target. Continue weekly RCA and burn-rate monitoring.")

    lines.append("2) **Schedule Recovery**")
    if spi < spi_thr or red_spi > 0:
        lines.append(
            f"   • SPI < {spi_thr:.2f}. Crash/fast-track critical path; OT/weekend shifts with best crash ratio."
        )
        lines.append("   • Resequence near-critical paths; expedite critical materials.")
    else:
        lines.append("   • SPI acceptable. Maintain rolling-wave updates and float monitoring.")

    delays = summary.get("procurement_top_delays", [])
    lines.append("3) **Procurement Acceleration**")
    if delays:
        lines.append("   • Daily expeditor huddles; enforce LDs; line up alternates for top late items.")
    else:
        lines.append("   • On-time vendors. Keep scorecards and SLAs enforced.")

    mc = summary.get("monte_carlo", [])
    risky = [
        m
        for m in mc
        if m.get("EAC_P80", 0) > 1.05 * m.get("EAC_P50", 0) or m.get("Finish_P80", 0) > m.get("Finish_P50", 0) + 10
    ]
    lines.append("4) **Forecasting Discipline**")
    if risky:
        lines.append("   • Where P80 ≫ P50, brief execs on **P80**; fund mitigations from contingency.")
    else:
        lines.append("   • P50/P80 spread controlled; maintain trend checks and hold P80 in reserve.")

    lines.append("5) **Cadence & Accountability**")
    lines.append("   • Weekly batch: auto-create JIRA for CPI/SPI reds or PO delay > 10d with owner & due date.")
    return "\n".join(lines)


def try_llm_then_rules(summary: Dict[str, Any], temperature: float = 0.2) -> Tuple[str, str]:
    """
    Try cloud LLMs (OpenAI/Anthropic/Groq) if keys exist; otherwise return rule-based text.
    Returns (text, source_model).
    """
    prompt = (
        "You are a Principal Project Controls AI. Summarize risks and produce a numbered action plan "
        "to prevent cost overruns and schedule slips, referencing CPI/SPI thresholds, procurement delays, "
        "and P50/P80 Monte Carlo. Limit to ~200 words.\n\n"
        f"{json.dumps(summary, indent=2)}"
    )

    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    groq_key = os.getenv("GROQ_API_KEY", "").strip()

    try:
        # --- OpenAI path -----------------------------------------------------
        if openai_key:
            from openai import OpenAI

            client = OpenAI(api_key=openai_key)
            model = os.getenv("LLM_MODEL", "gpt-4o-mini")
            openai_resp = client.chat.completions.create(
                model=model,
                temperature=float(temperature),
                messages=[
                    {"role": "system", "content": "You are a Principal Project Controls AI."},
                    {"role": "user", "content": prompt},
                ],
            )
            text = (openai_resp.choices[0].message.content or "").strip()
            return text, f"OpenAI ({model})"

        # --- Anthropic path --------------------------------------------------
        if anthropic_key:
            import anthropic

            model = os.getenv("LLM_MODEL", "claude-3-5-sonnet-20240620")
            c = anthropic.Anthropic(api_key=anthropic_key)
            anthropic_msg = c.messages.create(
                model=model,
                max_tokens=600,
                temperature=float(temperature),
                system="You are a Principal Project Controls AI.",
                messages=[{"role": "user", "content": prompt}],
            )
            parts = [getattr(p, "text", "") for p in anthropic_msg.content]
            text = "".join(parts).strip()
            return text, f"Anthropic ({model})"

        # --- Groq path -------------------------------------------------------
        if groq_key:
            from groq import Groq

            model = os.getenv("LLM_MODEL", "llama-3.1-70b-versatile")
            g = Groq(api_key=groq_key)
            groq_resp = g.chat.completions.create(
                model=model,
                temperature=float(temperature),
                messages=[
                    {"role": "system", "content": "You are a Principal Project Controls AI."},
                    {"role": "user", "content": prompt},
                ],
            )
            text = (groq_resp.choices[0].message.content or "").strip()
            return text, f"Groq ({model})"

    except Exception as e:
        # If any cloud call fails, fall back gracefully with rule-based text
        fb = rule_based_recs(summary)
        return f"{fb}\n\n_Provisioned LLM call failed: {e}_", "Rule-based (LLM fallback)"

    # No keys configured → rule-based fallback
    return rule_based_recs(summary), "Rule-based expert system"


# ──────────────────────────────────────────────────────────────────────────────
# 7) MAIN TABS — KPIs, Forecast, Procurement, Alerts, AI Copilot
# ──────────────────────────────────────────────────────────────────────────────
tab_kpi, tab_fc, tab_proc, tab_alerts, tab_ai = st.tabs(["KPIs", "Forecast", "Procurement", "Alerts", "AI Copilot"])

# --- KPIs TAB ---------------------------------------------------------------
with tab_kpi:
    st.subheader("Portfolio KPIs (latest per WBS)")
    latest = evm_df.copy()
    if "Period" in latest.columns:
        latest["Period"] = pd.to_datetime(latest["Period"], errors="coerce")
        latest = latest.sort_values("Period").groupby(["ProjectID", "WBS"], as_index=False).tail(1)

    c1, c2, c3, c4 = st.columns(4)
    if "CPI" in latest.columns and len(latest):
        c1.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Avg CPI</div>'
            f'<div class="kpi-value">{latest["CPI"].mean():.2f}</div></div>',
            unsafe_allow_html=True,
        )
    if "SPI" in latest.columns and len(latest):
        c2.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Avg SPI</div>'
            f'<div class="kpi-value">{latest["SPI"].mean():.2f}</div></div>',
            unsafe_allow_html=True,
        )
    if "EAC" in latest.columns and len(latest):
        c3.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Total EAC</div>'
            f'<div class="kpi-value">${latest["EAC"].sum():,.0f}</div></div>',
            unsafe_allow_html=True,
        )
    if "VAC" in latest.columns and len(latest):
        c4.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Total VAC</div>'
            f'<div class="kpi-value">${latest["VAC"].sum():,.0f}</div></div>',
            unsafe_allow_html=True,
        )

    show_cols = [c for c in ["ProjectID", "WBS", "CPI", "SPI", "EAC", "VAC", "BAC", "Period"] if c in latest.columns]
    st.dataframe(latest[show_cols], use_container_width=True)

# --- FORECAST TAB -----------------------------------------------------------
with tab_fc:
    st.subheader("Forecast (Monte Carlo)")
    proj_ids = sorted(mc_df["ProjectID"].dropna().unique().tolist())
    selected_proj = st.selectbox("Select Project", proj_ids, index=0 if proj_ids else None)

    row = mc_df[mc_df["ProjectID"] == selected_proj].head(1)
    c1, c2, c3, c4 = st.columns(4)
    if not row.empty:
        c1.markdown(
            f'<div class="kpi-card"><div class="kpi-label">EAC P50</div>'
            f'<div class="kpi-value">${row["EAC_P50"].iloc[0]:,.0f}</div></div>',
            unsafe_allow_html=True,
        )
        c2.markdown(
            f'<div class="kpi-card"><div class="kpi-label">EAC P80</div>'
            f'<div class="kpi-value">${row["EAC_P80"].iloc[0]:,.0f}</div></div>',
            unsafe_allow_html=True,
        )
        c3.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Finish P50 (days)</div>'
            f'<div class="kpi-value">{row["Finish_P50"].iloc[0]:,.1f} d</div></div>',
            unsafe_allow_html=True,
        )
        c4.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Finish P80 (days)</div>'
            f'<div class="kpi-value">{row["Finish_P80"].iloc[0]:,.1f} d</div></div>',
            unsafe_allow_html=True,
        )

    st.divider()
    # S-curve for EAC (CDF)
    if scurve_df is not None and len(scurve_df):
        scurve_df = cast(pd.DataFrame, scurve_df)  # narrow Optional for mypy
        sc_proj = scurve_df[(scurve_df["ProjectID"] == selected_proj) & (scurve_df["Metric"] == "EAC")]
        if len(sc_proj):
            fig_sc = px.line(
                sc_proj.sort_values("Value"),
                x="Value",
                y="CDF",
                title=f"EAC S-curve (CDF) — {selected_proj}",
                labels={"Value": "EAC ($)", "CDF": "Cumulative Probability"},
            )
            fig_sc.update_layout(yaxis=dict(range=[0, 1]))
            st.plotly_chart(fig_sc, use_container_width=True)
        else:
            st.info("No S-curve points for this project.")
    else:
        st.info("S-curve file not found (forecast_s_curves.parquet).")

    st.divider()
    # Histogram: finish days over baseline
    if runs_df is not None and len(runs_df):
        runs_df = cast(pd.DataFrame, runs_df)  # narrow Optional for mypy
        rproj = runs_df[runs_df["ProjectID"] == selected_proj]
        if len(rproj):
            fig_hist = px.histogram(
                rproj,
                x="FinishDaysOverBaseline",
                nbins=30,
                title=f"Finish Days over Baseline — Distribution — {selected_proj}",
                labels={"FinishDaysOverBaseline": "Days over baseline"},
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("No Monte Carlo runs for this project.")
    else:
        st.info("Runs file not found (monte_carlo_runs.parquet).")

# --- PROCUREMENT TAB --------------------------------------------------------
with tab_proc:
    st.subheader("Procurement Impacts")
    show_proc = proc_df.loc[:, [c for c in proc_df.columns if c != "Unnamed: 0"]]
    st.dataframe(show_proc, use_container_width=True)

# --- ALERTS TAB -------------------------------------------------------------
with tab_alerts:
    st.subheader("Alerts Outbox (dry-run history)")
    if alerts_text:
        try:
            at = alerts_text.strip()
            if at.startswith("{") or at.startswith("["):
                st.json(json.loads(at))  # pretty JSON if possible
            else:
                st.text(alerts_text)
        except Exception:
            st.text(alerts_text)
    else:
        st.info("No alerts yet. Generate in AI Copilot tab or via:\n`python -m services.alerts`")

# --- AI COPILOT TAB ---------------------------------------------------------
with tab_ai:
    st.subheader("AI Copilot — Proactive Recommendations")
    st.caption("Analyzes CPI/SPI, P50/P80, and procurement to suggest actions to prevent overruns and delays.")

    # Optional: choose projects to analyze
    plist = sorted(evm_df["ProjectID"].dropna().unique().tolist())
    selected_projects = st.multiselect("Projects to analyze", plist, default=plist)

    # Initialize session state fields
    if "ai_text" not in st.session_state:
        st.session_state.ai_text = ""
        st.session_state.ai_source = "n/a"

    # Generate the AI recommendations (LLM if keys available, else rule-based)
    if st.button("⚡ Generate AI Recommendations", use_container_width=True):
        with st.spinner("Analyzing portfolio…"):
            summary = build_portfolio_summary(evm_df, mc_df, proc_df, projects=selected_projects)
            ai_text, used_model = try_llm_then_rules(summary, temperature=float(CFG["ai"]["temperature"]))
            st.session_state.ai_text = ai_text
            st.session_state.ai_source = used_model
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
            AI_NOTE_FP.write_text(ai_text, encoding="utf-8")
        st.success("AI recommendations generated (also saved to data/processed/ai_recommendations.txt).")

    if st.session_state.ai_text:
        st.write(st.session_state.ai_text)
        st.caption(f"Source: {st.session_state.ai_source}")
    else:
        st.info("Click **Generate AI Recommendations** to produce the summary and action plan.")

    st.divider()
    st.markdown("### Send Alerts & Create JIRA Ticket(s)")
    st.caption("Uses your alert pipeline. With `alerts.dry_run: true`, everything is simulated safely.")

    colA, colB, colC = st.columns(3)
    with colA:
        use_jira = st.checkbox("JIRA", value=True)
    with colB:
        use_slack = st.checkbox("Slack", value=False)
    with colC:
        use_email = st.checkbox("Email", value=False)

    send_disabled = not (use_jira or use_slack or use_email)
    if st.button("📨 Create Ticket(s) from AI Plan", use_container_width=True, disabled=send_disabled):
        with st.spinner("Dispatching alerts via services.alerts…"):
            python = sys.executable
            base_cmd = [python, "-m", "services.alerts"]
            if use_jira:
                base_cmd.append("--jira")
            if use_slack:
                base_cmd.append("--slack")
            if use_email:
                base_cmd.append("--email")

            # Prefer attaching the AI note file if CLI supports --note
            cmd_with_note = base_cmd + ["--note", str(AI_NOTE_FP)]
            code, out, err = run_command(cmd_with_note, cwd=ROOT)

            if code != 0:
                # Retry without --note for older CLI compatibility
                code2, out2, err2 = run_command(base_cmd, cwd=ROOT)
                if code2 == 0:
                    st.warning("Alert pipeline ran without attaching the AI note (CLI may not support --note).")
                    st.success("Alerts dispatched (check alerts_outbox.json).")
                    with st.expander("Alert output (retry without --note)"):
                        st.code(out2 or "<no stdout>")
                        if err2:
                            st.code(err2)
                else:
                    st.error("Alert pipeline failed. See logs below.")
                    with st.expander("services.alerts output (both attempts)"):
                        st.markdown("**Attempt with --note:**")
                        st.code(out or "<no stdout>")
                        if err:
                            st.code(err)
                        st.markdown("**Attempt without --note:**")
                        st.code(out2 or "<no stdout>")
                        if err2:
                            st.code(err2)
            else:
                st.success("Alerts dispatched and (dry-run) ticket(s) created. See alerts_outbox.json.")
                with st.expander("services.alerts output"):
                    st.code(out or "<no stdout>")
                    if err:
                        st.code(err)
