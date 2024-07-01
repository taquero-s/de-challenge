import pytest
import requests

from src.hermes.producer import Producer


class TestProducer:
    @pytest.fixture(scope="function")
    def producer(self) -> Producer:
        return Producer()

    def test_pull_order_books(self, producer: Producer):
        """Tests order book is successfully pulled"""
        response = producer.pull_order_book("btc_mxn")

        assert sorted(response.keys()) == ["asks", "bids", "sequence", "updated_at"]

    def test_raise_unknown_order_book(self, producer: Producer):
        """Test errors are raised whenever an unknown book is set"""
        with pytest.raises(requests.HTTPError):
            producer.pull_order_book("unknown")
