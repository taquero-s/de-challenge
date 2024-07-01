"""Hermes data consumer
"""

import datetime
import json
import logging
from pathlib import Path


class Consumer:
    """Consumer class used to write data to a local file system in json format.

    Args:
        path (Path): Path to which the data will be stored into.
        fmt (str, optional): File prefix used to organize outgoing events.
            Defaults to "%Y/%m/%d/%H".
    """

    def __init__(self, path: Path | str, fmt: str = "%Y/%m/%d/%H") -> None:
        if isinstance(path, str):
            path = Path(path)

        logging.info("Storing data in path: %s", path)
        self.path = path
        self.fmt = fmt

    def push_records(self, records: list[dict]) -> None:
        """Writes the incoming records into the configured file system. Data is
        as ndjson into batch files organized with their timestamp values.

        Args:
            records (list[dict]): List of json payloads.
        """
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc)
        utc_timestamp = utc_timestamp.replace(microsecond=0)
        path = self.path / utc_timestamp.strftime(self.fmt)
        path.mkdir(parents=True, exist_ok=True)

        file = path / "batch_{}.json".format(int(utc_timestamp.timestamp() * 1000))

        with file.open("w", encoding="utf-8") as _out:
            for _r in records:
                _out.write(json.dumps(_r))
                _out.write("\n")
