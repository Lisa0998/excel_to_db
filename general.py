import clickhouse_connect
import pandas as pd
from clickhouse_driver import Client
import os as os

path = 'resource'

def get_all_paths(path) -> list[str]:
    list_of_files = os.listdir(path)
    return list_of_files


def create_df_from_path(path: str) -> pd.DataFrame:
    dataframe = pd.read_excel(path)
    return dataframe


# client = Client('localhost',
#                 user='lisa',
#                 password='fox',
#                 secure=False,
#                 verify=False,
#                 database='test',
#                 compression=False,
#                 settings={"use_numpy":True})

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
    DROP TABLE if exists data_from_xlsx
    """)

    client.command("""
    CREATE TABLE data_from_xlsx
        (calculation_date Date,
        credit_id Int64,
        initial_amount Float64,
        credit_count_days Int64,
        status String,
        status_days_count Int64,
        date_received Date,
        due_date Date,
        hard_v2 Int64,
        amount_of_days Int64,
        rest_ref Int64,
        bankrupt_flg Int64,
        PDorIL String,
        reserve_percent Nullable(Float64),
        main_debt Nullable(Float64),
        percent_debt Nullable(Float64),
        Main_debt_reserve Nullable(Float64),
        Percent_reserve Nullable(Float64)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(calculation_date) ORDER BY (calculation_date, date_received)
    """)

create_table()

def insert_data(data: pd.DataFrame):
    client.insert_df("data_from_xlsx", data)


for i in get_all_paths(path):
    print(i)
    name_file = ("resource/") + i
    df = create_df_from_path(name_file)
    insert_data(df)


