"""Main module for data stream.
"""

import concurrent.futures as cf
import logging
import time
from pathlib import Path

import typer

from .consumer import Consumer
from .producer import Producer

app = typer.Typer()

BOOKS = ["btc_mxn", "usd_mxn"]


def job(producer: Producer, consumer: Consumer):
    """Executes EL job to extract data from the producer and load it into the
    consumer

    Args:
        producer (Producer): Class used to extract data from.
        consumer (Consumer): Class used to load data into.
    """
    data = []
    for book in BOOKS:
        data.append(producer.pull_order_book(book))

    logging.info("Loading %s messages into consumer.", len(data))
    consumer.push_records(data)


@app.command()
def run(path: Path, env: str = "dev", seconds: int = 1):
    """Main function used for interaction between CLI and actual code.

    Args:
        path (Path): File system path to which the data will be loaded into.
        env (str, optional): Environment from which data will be loaded from.
            Defaults to "dev".
        seconds (int, optional): Seconds interval for API consumption. Defaults
            to 1.
    """
    producer = Producer(env)
    consumer = Consumer(path.expanduser())

    with cf.ThreadPoolExecutor(max_workers=2) as executor:
        try:
            while True:
                executor.submit(job, producer, consumer)
                time.sleep(seconds)
        except KeyboardInterrupt as e:
            raise e
        finally:
            logging.info("Gracefully shutting down job.")
            executor.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    app()
