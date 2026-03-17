import os
from dotenv import load_dotenv
from transform_data import WeeklyReports
import logging
from database import MSSQL
import time
from prefect import task
import os

logging.basicConfig(
    filename="load_weekly_files.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_weekly_files")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = [os.getenv("WEEKLY_TABLE_NAME")]
    ms_sql.truncate_tables(table_list=table_list)

    weekly_files = os.getenv("WEEKLY_FILE_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("WEEKLY_TABLE_NAME")} table'
    )
    start_time = time.time()
    for file in os.listdir(weekly_files):
        file_path = os.path.join(weekly_files, file)
        weekly_data = WeeklyReports(file_path)
        count = weekly_data.structurize_data()
        if count == 1:
            ms_sql.insert_dataframe(os.getenv("WEEKLY_TABLE_NAME"), weekly_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("WEEKLY_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info("The script was successfull")
