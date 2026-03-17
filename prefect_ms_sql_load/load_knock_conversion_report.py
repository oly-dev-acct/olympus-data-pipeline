import os
from dotenv import load_dotenv
from transform_data import KnockCoversionReport
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_knock_conversion_report.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_knock_conversion_report")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("KNOCK_CONVERSION_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("KNOCK_CONVERSION_TABLE_NAME")} table'
    )
    directory = os.getenv("KNOCK_CONVERSION_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "conversion".lower() in file.lower():
            knock_conversion_report = KnockCoversionReport(full_path)
            knock_conversion_report.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("KNOCK_CONVERSION_TABLE_NAME"),
                knock_conversion_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("KNOCK_CONVERSION_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
