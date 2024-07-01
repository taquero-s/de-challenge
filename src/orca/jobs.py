from dagster import (
    AssetSelection,
    HookContext,
    build_schedule_from_partitioned_job,
    define_asset_job,
    failure_hook,
)

from src.orca.assets.order_book_report import PARTITION_DEF, order_book_report
from src.orca.assets.user_events import (
    dim_users,
    fct_logins_summary,
    fct_transactions_summary,
)


@failure_hook()
def send_failure_notification(context: HookContext):
    pass
    # message = f"Op {context.op.name} failed"
    # context.resources.slack.chat_postMessage(channel="#foo", text=message)


order_books_report__job = define_asset_job(
    name="order_books_report__job",
    selection=AssetSelection.assets(order_book_report),
    partitions_def=PARTITION_DEF,
    hooks={send_failure_notification},
)
order_books_report__schedule = build_schedule_from_partitioned_job(
    order_books_report__job,
)

user_master_data__job = define_asset_job(
    name="user_master_data__job",
    selection=AssetSelection.assets(
        fct_logins_summary, fct_transactions_summary, dim_users
    ).upstream(),
    partitions_def=PARTITION_DEF,
    hooks={send_failure_notification},
)
