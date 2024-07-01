from enum import Enum

import requests


class BitsoHost(str, Enum):
    DEV = "https://stage.bitso.com/api/v3"
    PRD = "https://bitso.com/api/v3"


class Producer:
    def __init__(self, env: str = "dev") -> None:
        self.host = BitsoHost[env.upper()]

    def pull_order_books(self, book: str) -> dict:
        endpoint = self.host + "/order_book"
        params = {"book": book}
        response = requests.get(endpoint, params=params, timeout=1000)
        response.raise_for_status()

        return response.json()["payload"]
