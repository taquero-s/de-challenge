include .env

check_env:
	@if [ ! -f .env ]; then \
		echo ".env file not found"; \
		exit 1; \
	fi
	@if ! grep -q 'DATA_DIR=' .env; then \
		echo "DATA_DIR variable not found in .env file"; \
		exit 1; \
	fi
	@echo ".env file exists and DATA_DIR variable is set"

dev:
	@poetry install --no-root

test:
	@poetry run pytest ./tests

hermes-run: check_env
	@poetry run python -m src.hermes ${DATA_DIR}/json_order_book --env prd

orca-start: check_env
	@poetry run dagster dev
