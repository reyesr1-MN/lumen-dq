.DEFAULT_GOAL := help

PYTHON   := python
DATA_DIR := data/baseline

.PHONY: help install generate checks validate test demo clean

help:
	@echo ""
	@echo "  Lumen Community Bank — Data Quality Demo"
	@echo ""
	@echo "  Setup"
	@echo "    make install    Install locked dependencies into current virtualenv"
	@echo ""
	@echo "  Core pipeline"
	@echo "    make generate   Write deterministic synthetic parquet files → data/baseline/"
	@echo "    make checks     Run all layered DQ checks (expects PASS on baseline)"
	@echo "    make validate   Run executable accounts YAML contract validator"
	@echo "    make test       Run full pytest suite (generates data first)"
	@echo ""
	@echo "  Incident demo"
	@echo "    make demo       Record full demonstration → evidence/"
	@echo ""
	@echo "  Housekeeping"
	@echo "    make clean      Remove generated data and evidence logs"
	@echo ""

install:
	pip install -r requirements.lock
	pip install -e .

generate:
	$(PYTHON) -m lumen_dq.generate --output-dir $(DATA_DIR)

checks: generate
	$(PYTHON) -m lumen_dq.run_checks --data-dir $(DATA_DIR)

validate: generate
	$(PYTHON) -m lumen_dq.validate_contract contracts/accounts.yml \
	    --data-dir $(DATA_DIR)

test: generate
	pytest tests/ -v --tb=short --cov=lumen_dq --cov-report=term-missing

demo: generate
	bash scripts/record_demo.sh

clean:
	rm -rf $(DATA_DIR)
	rm -f evidence/demo_*.txt evidence/evidence_*.json evidence/junit.xml
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
