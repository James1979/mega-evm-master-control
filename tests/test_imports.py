"""
tests/test_imports.py

Smoke test to ensure all ETL/Services modules import without errors.
This catches syntax or import issues early in CI pipelines.
"""

import importlib


def test_import_etl_and_services_modules() -> None:
    """Loops through module names and imports each one."""
    modules = [
        "etl.evm_calculator",
        "etl.monte_carlo",
        "etl.p6_ingest",
        "etl.procurement_join",
        "services.alerts",
        "services.ai_variance_narratives",
    ]
    for mod in modules:
        importlib.import_module(mod)
