import json
from pathlib import Path

import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator


BASE_DATA = pd.DataFrame(
    {
        "revenue_gross": [100.0, 200.0, 150.0],
        "option_net": [10.0, 20.0, 5.0],
        "revenue_net": [110.0, 220.0, 155.0],
    }
)


def dynamic_expectations(df: pd.DataFrame) -> None:
    """Build expectations from BASE_DATA and validate df."""
    ge.get_context()
    engine = PandasExecutionEngine()
    batch = Batch(data=df.copy())
    validator = Validator(execution_engine=engine, batches=[batch])

    # row count must match baseline
    validator.expect_table_row_count_to_equal(len(BASE_DATA))

    # derive null thresholds and numeric ranges from baseline
    numeric_cols = BASE_DATA.select_dtypes(include="number").columns
    for col in BASE_DATA.columns:
        null_rate = BASE_DATA[col].isnull().mean()
        if null_rate == 0:
            validator.expect_column_values_to_not_be_null(column=col)
        else:
            validator.expect_column_proportion_of_nulls_to_be_between(
                column=col, min_value=0, max_value=null_rate
            )
        if col in numeric_cols:
            validator.expect_column_values_to_be_between(
                column=col,
                min_value=BASE_DATA[col].min(),
                max_value=BASE_DATA[col].max(),
            )

    results = validator.validate()
    assert results.success, results

    # Business rule: revenue_net should equal revenue_gross + option_net
    df2 = df.copy()
    df2["expected_net"] = df2["revenue_gross"] + df2["option_net"]
    validator2 = Validator(
        execution_engine=PandasExecutionEngine(), batches=[Batch(data=df2)]
    )
    validator2.expect_column_pair_values_to_be_equal(
        column_A="revenue_net", column_B="expected_net"
    )
    result2 = validator2.validate()
    assert result2.success, result2

def run_suite(suite_file: Path, df: pd.DataFrame) -> None:
    """Validate df against an expectation suite then run dynamic checks."""
    with suite_file.open() as f:
        suite = json.load(f)

    ge.get_context()

    engine = PandasExecutionEngine()
    batch = Batch(data=df.copy())
    validator = Validator(execution_engine=engine, batches=[batch])

    for exp in suite["expectations"]:
        exp_type = exp["expectation_type"]
        kwargs = exp["kwargs"]
        getattr(validator, exp_type)(**kwargs)

    results = validator.validate()
    assert results.success, results

    # Follow-up dynamic expectations
    dynamic_expectations(df)


def test_raw_table():
    run_suite(Path(__file__).with_name("raw_table_suite.json"), BASE_DATA)


def test_silver_table():
    run_suite(Path(__file__).with_name("silver_table_suite.json"), BASE_DATA)


def test_gold_table():
    run_suite(Path(__file__).with_name("gold_table_suite.json"), BASE_DATA)
