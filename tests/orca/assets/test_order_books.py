from pandas import DataFrame
from pandas.testing import assert_frame_equal

from src.orca.assets.order_books import build_sparse_report


def test_build_sparse_report():
    """Test a sparse report is generated as expected"""
    records = [
        {
            "asks": [
                {"book": "btc_mxn", "price": "5632.24", "amount": "1.34491802"},
                {"book": "btc_mxn", "price": "5633.44", "amount": "0.4259"},
                {"book": "btc_mxn", "price": "5642.14", "amount": "1.21642"},
            ],
            "bids": [
                {"book": "btc_mxn", "price": "6123.55", "amount": "1.12560000"},
                {"book": "btc_mxn", "price": "6121.55", "amount": "2.23976"},
            ],
            "updated_at": "2016-04-08T17:52:31.000+00:00",
            "sequence": "27214",
        }
    ]

    pdf = DataFrame.from_records(records)
    report = build_sparse_report(pdf)
    expected = DataFrame(
        {
            "orderbook_timestamp": ["2016-04-08T17:52:31.000+00:00"],
            "book": ["btc_mxn"],
            "bid": [6123.55],
            "ask": [5632.24],
            "spread": [-8.7232],
        }
    )

    assert_frame_equal(report, expected, rtol=0.0001)
