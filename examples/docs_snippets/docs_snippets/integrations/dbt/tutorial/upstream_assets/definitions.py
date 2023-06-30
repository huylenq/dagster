import os

import duckdb
import pandas as pd

from dagster import asset, file_relative_path

DBT_PROJECT_PATH = file_relative_path(__file__, "../../jaffle_shop")
DBT_PROFILES = file_relative_path(__file__, "../../jaffle_shop/config")

DUCKDB_DATABASE_PATH = os.path.join(DBT_PROJECT_PATH, "tutorial.duckdb")


@asset
def raw_customers(context) -> None:
    data = pd.read_csv("https://docs.dagster.io/assets/customers.csv")
    connection = duckdb.connect(DUCKDB_DATABASE_PATH)
    connection.execute("create schema if not exists jaffle_shop")
    connection.execute("create or replace table jaffle_shop.raw_customers as select * from data")

    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": data.shape[0]})


@asset
def raw_orders(context) -> None:
    data = pd.read_csv("https://docs.dagster.io/assets/orders.csv")
    connection = duckdb.connect(DUCKDB_DATABASE_PATH)
    connection.execute("create schema if not exists jaffle_shop")
    connection.execute("create or replace table jaffle_shop.raw_orders as select * from data")

    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": data.shape[0]})
