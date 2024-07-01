"""Hermes data producers
"""

import logging
from enum import Enum

import requests


class BitsoHost(str, Enum):
    """Enumerator for bitso hosts."""

    DEV = "https://stage.bitso.com/api/v3"
    PRD = "https://bitso.com/api/v3"


class Producer:
    """Producer class used to pull data from bitso host.

    Args:
        env (str, optional): Environment to pull date from. Available values are
            dev and prod. Defaults to "dev"
    """

    def __init__(self, env: str = "dev") -> None:

        host = BitsoHost[env.upper()]
        logging.info("Reading data from host: %s", host)

        self.host = host

    def pull_order_book(self, book: str) -> dict:
        """Sends a request to the bitso order_book endpoint to pull the latest
        information of the open orders of a specific book.

        Args:
            book (str): Book to pull data for.

        Returns:
            dict: Endpoint response payload.
        """
        endpoint = self.host + "/order_book"
        params = {"book": book}
        response = requests.get(endpoint, params=params, timeout=1000)
        response.raise_for_status()

        return response.json()["payload"]
