"""Dagster framework initialization"""

from dagster import (
    Definitions,
    EnvVar,
    load_asset_checks_from_package_module,
    load_assets_from_package_module,
)
from upath import UPath

from . import assets, jobs, resources

DATA_DIR = UPath(EnvVar("DATA_DIR").get_value()).expanduser()
defs = Definitions(
    assets=load_assets_from_package_module(assets),
    asset_checks=load_asset_checks_from_package_module(assets),
    resources={
        "fs_resource": resources.FilesystemResource(base_dir=str(DATA_DIR)),
        "io_manager": resources.PandasCsvIOManager(DATA_DIR),
    },
    jobs=[jobs.challenge_2__job],
    schedules=[jobs.challenge_1__schedule],
)
