import os
from dotenv import load_dotenv
from transform_data import ServiceRequestData
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_service_requests_new_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_service_requests_new_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
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
    ps_sql.insert_dataframe_copy_command(os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME"), service_request_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
