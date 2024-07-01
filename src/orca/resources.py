from pathlib import Path

from dagster import ConfigurableResource, InputContext, OutputContext, UPathIOManager
from pandas import DataFrame as PandasDF
from pandas import read_csv
from upath import UPath


class FilesystemResource(ConfigurableResource):
    base_dir: str

    @property
    def path(self) -> Path:
        return Path(self.base_dir)


class PandasCsvIOManager(UPathIOManager):
    extension: str = ".csv"

    def dump_to_path(self, context: OutputContext, obj: PandasDF, path: UPath) -> None:
        with path.open("w") as _f:
            obj.to_csv(_f, index=False)

    def load_from_path(self, context: InputContext, path: UPath) -> PandasDF:
        return read_csv(path)
