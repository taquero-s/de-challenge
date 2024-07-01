import datetime
import json
from pathlib import Path


class Consumer:
    def __init__(self, path: Path) -> None:
        self.path = path

    def push_records(self, records: list[dict]) -> None:
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc)
        utc_timestamp = utc_timestamp.replace(microsecond=0)
        path = self.path / utc_timestamp.strftime("%Y/%m/%d/%H")
        path.mkdir(parents=True, exist_ok=True)

        file = path / "batch_{}.json".format(int(utc_timestamp.timestamp() * 1000))

        with file.open("w", encoding="utf-8") as _out:
            for _r in records:
                _out.write(json.dumps(_r))
                _out.write("\n")
