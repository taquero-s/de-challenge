include .env

dev:
	@poetry install --no-root

test:
	@poetry run pytest ./tests

hermes-run:
	@poetry run python -m src.hermes ${DATA_DIR}/json_order_book --env prd

orca-start:
	@poetry run dagster dev
