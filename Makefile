dev:
	poetry install --no-root

hermes-run:
	poetry run python -m src.hermes ~/Data/order_books --env prd

orca-start:
	poetry run dagster dev
