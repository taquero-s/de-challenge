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
    data = []
    for book in BOOKS:
        data.append(producer.pull_order_books(book))

    logging.info("Loading %s messages into consumer.", len(data))
    consumer.push_records(data)


@app.command()
def run(path: Path, env: str = "dev", seconds: int = 1):
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
            executor.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    app()
