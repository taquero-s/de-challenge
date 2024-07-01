import pytest
from pandas import DataFrame as PandasDF
from pandas.testing import assert_frame_equal

from src.hermes.consumer import DuckDBConsumer


class TestDuckDBConsumer:
    @pytest.fixture(scope="function")
    def consumer(self) -> DuckDBConsumer:
        consumer = DuckDBConsumer()

        return consumer

    def test_push_dataframe_on_pre_existent(self, consumer: DuckDBConsumer):
        consumer.conn.sql("CREATE TABLE temporary AS SELECT 42 AS num")

        pdf = PandasDF(data={"num": [43]})
        consumer.push("temporary", pdf)

        assert_frame_equal(
            consumer.conn.sql("SELECT * FROM temporary").df(),
            PandasDF(data={"num": [42, 43]}),
            check_dtype=False,
        )

    def test_push_dataframe_on_non_existent(self, consumer: DuckDBConsumer):
        pdf = PandasDF(data={"num": [42, 43]})
        consumer.push("temporary", pdf)

        assert_frame_equal(
            consumer.conn.sql("SELECT * FROM temporary").df(),
            PandasDF(data={"num": [42, 43]}),
            check_dtype=False,
        )

    def test_push_to_custom_schema(self, consumer: DuckDBConsumer):
        consumer.schema = "analytics"
        pdf = PandasDF(data={"num": [42, 43]})
        consumer.push("temporary", pdf)

        assert_frame_equal(
            consumer.conn.sql("SELECT * FROM analytics.temporary").df(),
            PandasDF(data={"num": [42, 43]}),
            check_dtype=False,
        )
