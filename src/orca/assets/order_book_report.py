import datetime
from pathlib import Path

import duckdb
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetExecutionContext,
    TimeWindowPartitionsDefinition,
    asset,
    asset_check,
)
from pandas import DataFrame as PandasDF

from src.orca.resources import FilesystemResource

PARTITION_DEF = TimeWindowPartitionsDefinition(
    start=datetime.datetime.now().replace(tzinfo=None),
    cron_schedule="*/10 * * * *",
    fmt="report_date=%Y-%m-%d/%H%M",
)


@asset(
    io_manager_key="pandas_csv_io_manager",
    partitions_def=PARTITION_DEF,
    group_name="challenge_1",
)
def order_book_report(
    context: AssetExecutionContext,
    fs_resource: FilesystemResource,
) -> PandasDF:
    """Report that displays the bid-ask spread from order books second by second.
    This report is generated once every 10 minutes.
    """
    start = context.partition_time_window.start
    end = context.partition_time_window.end
    data_dir: Path = fs_resource.path
    pdf = duckdb.sql(
        """select * from read_json_auto($dir) where updated_at between $start and $end""",
        params={
            "dir": str(data_dir / "order_books/**/*.json"),
            "start": start.strftime("%Y-%m-%dT%H:%M"),
            "end": end.strftime("%Y-%m-%dT%H:%M"),
        },
    ).df()
    pdf = pdf.drop_duplicates(subset=["updated_at", "sequence"])
    pdf = pdf.assign(
        book=pdf.bids.apply(lambda x: x[0]["book"]),
        best_bid=pdf.bids.apply(lambda x: max([float(b["price"]) for b in x])),
        best_ask=pdf.asks.apply(lambda x: min([float(a["price"]) for a in x])),
    )
    pdf["spread"] = ((pdf.best_ask - pdf.best_bid) * 100) / pdf.best_ask
    pdf = pdf[["updated_at", "book", "best_bid", "best_ask", "spread"]]
    pdf = pdf.rename(
        columns={
            "updated_at": "orderbook_timestamp",
            "best_bid": "bid",
            "best_ask": "ask",
        }
    )

    return pdf


@asset_check(asset=order_book_report)
def spread_bigger_than_threshold(
    context: AssetCheckExecutionContext, fs_resource: FilesystemResource
) -> AssetCheckResult:
    pdf = duckdb.sql(
        """select count(1) over_limit from read_csv_auto($dir) where spread > $threshold""",
        params={
            "dir": str(fs_resource.path / "order_book_report/**/*"),
            "threshold": 1.0,
        },
    ).to_df()
    over_threshold = pdf.loc[0].to_dict()["over_limit"]
    return AssetCheckResult(
        passed=(over_threshold == 0),
        metadata={"rows_over_limit": over_threshold},
    )
