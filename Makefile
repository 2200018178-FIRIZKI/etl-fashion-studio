# Makefile for ETL Pipeline - Fashion Studio
# Usage: make <command>

# Python command (sesuaikan jika perlu)
PYTHON = python
PYTEST = pytest

# Database config (ganti YOUR_PASSWORD dengan password PostgreSQL Anda)
DB_URL = postgresql://postgres:YOUR_PASSWORD@localhost:5433/fashion_studio

# =============================================================================
# MAIN COMMANDS
# =============================================================================

## Jalankan ETL pipeline lengkap (50 halaman)
run:
	$(PYTHON) main.py

## Jalankan ETL quick test (2 halaman)
quick:
	$(PYTHON) main.py --pages 1 2

## Jalankan ETL dengan PostgreSQL
run-db:
	$(PYTHON) main.py --postgresql "$(DB_URL)"

## Jalankan ETL quick test dengan PostgreSQL
quick-db:
	$(PYTHON) main.py --pages 1 2 --postgresql "$(DB_URL)"

# =============================================================================
# TESTING
# =============================================================================

## Jalankan semua unit tests
test:
	$(PYTEST) tests/ -v

## Jalankan tests dengan coverage report
coverage:
	$(PYTEST) tests/ -v --cov=utils --cov-report=term-missing

## Jalankan tests dengan HTML coverage report
coverage-html:
	$(PYTEST) tests/ -v --cov=utils --cov-report=html

# =============================================================================
# UTILITIES
# =============================================================================

## Install dependencies
install:
	pip install -r requirements.txt

## Clean cache files
clean:
	rm -rf __pycache__ .pytest_cache .coverage htmlcov
	rm -rf utils/__pycache__ tests/__pycache__

## Show help
help:
	@cat <<EOF
ETL Pipeline - Fashion Studio

Usage: make <command>

Main Commands:
  run        - Jalankan ETL pipeline lengkap (50 halaman)
  quick      - Jalankan ETL quick test (2 halaman)
  run-db     - Jalankan ETL dengan PostgreSQL
  quick-db   - Jalankan ETL quick test dengan PostgreSQL

Testing:
  test       - Jalankan semua unit tests
  coverage   - Jalankan tests dengan coverage report

Utilities:
  install    - Install dependencies
  clean      - Clean cache files
  help       - Show this help
EOF

.PHONY: run quick run-db quick-db test coverage coverage-html install clean help
