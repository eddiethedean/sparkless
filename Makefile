.PHONY: help install install-dev test test-cov lint format format-check type-check check-full clean build publish robin-parity-sql-internal

help: ## Show this help message
	@echo "Sparkless Package Management"
	@echo "============================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the package in development mode
	pip install -e .

install-dev: ## Install with development dependencies
	pip install -e ".[dev]"

test: ## Run tests (with Robin backend so skipifs for Robin limitations apply)
	SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh

test-cov: ## Run tests with coverage
	PYTEST_ADDOPTS="--cov=sparkless --cov-report=term-missing --cov-report=html --cov-report=xml" bash tests/run_all_tests.sh

lint: ## Run linting
	ruff check .

format: ## Format code
	ruff format .
	ruff check . --fix

format-check: ## Check formatting without modifying
	ruff format --check .
	ruff check .

type-check: ## Run mypy type checking
	mypy sparkless tests

check-full: format-check type-check install test ## Run full check suite (format, lint, types, compile+install, tests)

clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/

build: ## Build the package
	python -m build

publish: ## Publish to PyPI (requires authentication)
	twine upload dist/*

docs: ## Build documentation
	cd docs && sphinx-build -b html . _build/html

docs-clean: ## Clean documentation build
	rm -rf docs/_build/

docs-validate: ## Validate documentation accuracy
	python scripts/validate_docs_examples.py
	python scripts/verify_api_signatures.py
	python scripts/check_doc_accuracy.py

robin-parity-sql-internal: ## Run parity/sql + parity/internal in Robin; save to tests/robin_parity_sql_internal_results.txt
	SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest tests/parity/sql/ tests/parity/internal/ -v --tb=line -q 2>&1 | tee tests/robin_parity_sql_internal_results.txt

all: clean format type-check test build ## Run all checks and build
