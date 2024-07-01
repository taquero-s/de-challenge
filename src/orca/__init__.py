from dagster import (
    Definitions,
    load_asset_checks_from_package_module,
    load_assets_from_package_module,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager
from upath import UPath

from . import assets, jobs, resources

DATA_DIR = UPath("~/Data").expanduser()
defs = Definitions(
    assets=load_assets_from_package_module(assets),
    asset_checks=load_asset_checks_from_package_module(assets),
    resources={
        "fs_resource": resources.FilesystemResource(base_dir=str(DATA_DIR)),
        "pandas_csv_io_manager": resources.PandasCsvIOManager(DATA_DIR),
        "pandas_duckdb_io": DuckDBPandasIOManager(
            database=str(DATA_DIR / "warehouse.duckdb")
        ),
    },
    jobs=[jobs.user_master_data__job],
    schedules=[jobs.order_books_report__schedule],
)
