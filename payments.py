import clickhouse_connect
import pandas as pd
from clickhouse_driver import Client
import os as os

path = 'payments'

def get_all_paths(path) -> list[str]:
    list_of_files = os.listdir(path)
    return list_of_files


def create_df_from_path(path: str) -> pd.DataFrame:
    dataframe = pd.read_excel(path)
    return dataframe

client = clickhouse_connect.get_client(
    host='localhost',
    user='lisa',
    password='fox',
    port=8123,
    session_id='example_session_1',
    connect_timeout=15,
    database='test',
    distributed_ddl_task_timeout=300,
    compress=False,
)

def create_table():
    client.command("""
    DROP TABLE if exists zeo_payments
    """)

    client.command("""
    CREATE TABLE zeo_payments
        (id Int64,
        date_received Date,
        main_debt_repaid Nullable(Float64),
        percent_debt_repaid Nullable(Float64),
        insurance_for_client Nullable(Float64)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(date_received) ORDER BY (date_received)
    """)

create_table()

def insert_data(data: pd.DataFrame):
    print(data)
    client.insert_df("zeo_payments", data)


for i in get_all_paths(path):
    print(i)
    name_file = ("payments/") + i
    df = create_df_from_path(name_file)
    insert_data(df)


