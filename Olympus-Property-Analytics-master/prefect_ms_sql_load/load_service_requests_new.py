import os
from dotenv import load_dotenv
from transform_data import ServiceRequestData
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_service_requests_new.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_service_requests_new")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    service_request_report_data = ServiceRequestData(os.getenv("SERVICE_REQUESTS_REPORT_FOLDER_PATH"))
    service_request_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME"), service_request_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
