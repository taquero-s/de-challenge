import tempfile
from typing import Generator

import pytest

from src.hermes.consumer import Consumer


class TestConsumer:
    @pytest.fixture(scope="function")
    def consumer(self) -> Generator[Consumer, None, None]:
        with tempfile.TemporaryDirectory() as _tmp:
            consumer = Consumer(_tmp)

            yield consumer

    def test_push_to_filesystem(self, consumer: Consumer):
        """Test data is successfully pushed into a file system on ndjson format"""
        records = [
            {"message": 1},
            {"message": 2},
            {"message": 3},
        ]
        consumer.push_records(records)

        files = list(consumer.path.rglob("**/*.json"))

        assert len(files) == 1

        assert len(files[0].open().readlines()) == 3
