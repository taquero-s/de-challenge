import pytest
import requests

from src.hermes.producer import Producer


class TestBitsoProducer:
    @pytest.fixture(scope="function")
    def producer(self) -> Producer:
        return Producer()

    def test_pull_order_books(self, producer: Producer):
        response = producer.pull_order_books("btc_mxn")

        assert sorted(response.keys()) == ["asks", "bids", "sequence", "updated_at"]

    def test_raise_unknown_order_book(self, producer: Producer):
        with pytest.raises(requests.HTTPError):
            producer.pull_order_books("unknown")
