"""Definition of ELT jobs
"""

from dagster import (
    AssetSelection,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from src.orca.assets import order_books, user_events

################################################
# Challenge 1 Job

challenge_1__job = define_asset_job(
    name="challenge_1__job",
    selection=AssetSelection.assets(order_books.sparse_report),
    partitions_def=order_books.PARTITION_DEF,
)
challenge_1__schedule = build_schedule_from_partitioned_job(
    challenge_1__job,
)

################################################
# Challenge 2 Job

challenge_2__job = define_asset_job(
    name="challenge_2__job",
    selection=AssetSelection.assets(
        user_events.fct_logins_summary,
        user_events.fct_transactions_summary,
        user_events.dim_users,
    ).upstream(),
    partitions_def=user_events.PARTITION_DEF,
)
