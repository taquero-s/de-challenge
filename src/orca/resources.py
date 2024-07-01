"""Custom resources and IO managers for orchestration storage
"""

from pathlib import Path

from dagster import ConfigurableResource, InputContext, OutputContext, UPathIOManager
from pandas import DataFrame as PandasDF
from pandas import read_csv
from upath import UPath


class FilesystemResource(ConfigurableResource):
    """Resource to access file system resources."""

    base_dir: str

    @property
    def path(self) -> Path:
        return Path(self.base_dir)


class PandasCsvIOManager(UPathIOManager):
    """IO Manager used to write pandas dataframes as csv files"""

    extension: str = ".csv"

    def dump_to_path(self, context: OutputContext, obj: PandasDF, path: UPath) -> None:
        with path.open("w") as _f:
            obj.to_csv(_f, index=False)

    def load_from_path(self, context: InputContext, path: UPath) -> PandasDF:
        if path.exists():
            return read_csv(path)

        return PandasDF()
