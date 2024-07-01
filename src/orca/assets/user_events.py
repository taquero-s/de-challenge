import io
import zipfile

import duckdb
import requests
from dagster import DailyPartitionsDefinition, asset
from pandas import DataFrame as PandasDF
from pandas import read_csv

GROUP_NAME = "challenge_2"
PARTITION_DEF = DailyPartitionsDefinition(
    start_date="2020-01-02", end_date="2023-08-23"
)


def _get_response_io(url: str) -> io.BytesIO:
    response = requests.get(url, stream=True, timeout=1_000)

    return io.BytesIO(response.content)


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def deposits() -> PandasDF:
    url = (
        "https://github.com/IMARVI/sr_de_challenge/raw/main/deposit_sample_data.csv.zip"
    )
    with zipfile.ZipFile(_get_response_io(url)) as _zip:
        with _zip.open("deposit_sample_data.csv") as _f:
            pdf = read_csv(_f)

    return pdf


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def withdrawals() -> PandasDF:
    url = (
        "https://github.com/IMARVI/sr_de_challenge/raw/main/withdrawals_sample_data.csv"
    )

    return read_csv(_get_response_io(url))


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def events() -> PandasDF:
    url = "https://github.com/IMARVI/sr_de_challenge/raw/main/event_sample_data.csv"

    return read_csv(_get_response_io(url))


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def user_ids() -> PandasDF:
    url = "https://github.com/IMARVI/sr_de_challenge/raw/main/user_id_sample_data.csv"

    return read_csv(_get_response_io(url))


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def fct_transactions_summary(deposits: PandasDF, withdrawals: PandasDF) -> PandasDF:
    sql = """
    with _transactions_union as (
        select date_trunc('day', event_timestamp::timestamp) date,
            user_id,
            'withdrawal' transaction_type,
            interface,
            amount,
            currency,
            tx_status
        from withdrawals
        union
        select date_trunc('day', event_timestamp::timestamp) date,
            user_id,
            'deposit' transaction_type,
            NULL as interface,
            amount,
            currency,
            tx_status
        from deposits
    )
    select date,
        user_id,
        currency,
        sum(case when transaction_type = 'deposit' and tx_status = 'complete' then amount else 0 end) deposit_amt,
        sum(case when transaction_type = 'withdrawal' and tx_status = 'complete' then amount else 0 end) withdrawal_amt,
        sum(case when transaction_type = 'deposit' and tx_status = 'complete' then 1 else 0 end) deposit_tx,
        sum(case when transaction_type = 'withdrawal' and tx_status = 'complete' then 1 else 0 end) withdrawal_tx,
        count(1) total_tx
    from _transactions_union
    group by 1, 2, 3
    order by 1
    """
    return duckdb.sql(sql).to_df()


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def fct_logins_summary(events: PandasDF):
    sql = """
    select date_trunc('day', event_timestamp::timestamp) date,
        user_id,
        sum(case when event_name like '%login%' then 1 else 0 end) login_events
    from events
    group by 1, 2
    """

    return duckdb.sql(sql).to_df()


@asset(io_manager_key="pandas_duckdb_io", group_name=GROUP_NAME)
def dim_users(
    deposits: PandasDF, withdrawals: PandasDF, user_ids: PandasDF, events: PandasDF
) -> PandasDF:
    sql = """
    with _latest_deposits as (
        select user_id,
            max(event_timestamp::timestamp) latest_deposit_ts
        from deposits
        where tx_status = 'complete'
        group by 1
    ), _latest_withdrawals as (
        select user_id,
            max(event_timestamp::timestamp) latest_withdrawal_ts
        from withdrawals
        where tx_status = 'complete'
        group by 1
    ), _latest_logins as (
        select user_id,
            max(event_timestamp::timestamp) latest_login_ts
        from events
        where event_name like '%login%'
        group by 1
    )
    select u.user_id,
        ld.latest_deposit_ts,
        lw.latest_withdrawal_ts,
        ll.latest_login_ts
    from user_ids u
    left join _latest_deposits ld on ld.user_id = u.user_id
    left join _latest_withdrawals lw on lw.user_id = u.user_id
    left join _latest_logins ll on ll.user_id = u.user_id
    """

    return duckdb.sql(sql).to_df()
