    PY=python -m
    PIP=pip
    TEST=pytest -q --cov=./ --cov-report=term-missing --cov-fail-under=80

    .PHONY: setup dev test run audit sbom format type clean

    setup:
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt || true

    dev:
	streamlit run streamlit_app/app.py

    test:
	$(TEST)

    run:
	docker compose up --build

    audit:
	bandit -q -r .
	pip-audit -r requirements.txt

    sbom:
	pipdeptree --json-tree > sbom.json

    format:
	ruff check --fix .
	ruff format .
	mypy .

    clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml dist build
