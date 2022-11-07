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


client = Client('localhost',
                user='lisa',
                password='fox',
                secure=False,
                verify=False,
                database='test',
                compression=False,
                settings={"use_numpy":True})


for i in get_all_paths(path):
    print(i)
    name_file = ("resource/") + i
    df = create_df_from_path(name_file)
    insert_data(df)


