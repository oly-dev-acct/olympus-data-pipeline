import os
from dotenv import load_dotenv
from transform_data import SOCIReportDetails
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_soci_report_details.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_soci_report_details")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("SOCI_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("SOCI_REPORT_TABLE_NAME")} table'
    )
    directory = os.getenv("SOCI_REPORT_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "soci".lower() in file.lower():
            soci_report_data = SOCIReportDetails(full_path)
            soci_report_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("SOCI_REPORT_TABLE_NAME"),
                soci_report_data.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SOCI_REPORT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
