import os
from dotenv import load_dotenv
from transform_data import KnockCoversionReport
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_knock_conversion_report_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_knock_conversion_report_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
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
            ps_sql.insert_dataframe(
                os.getenv("KNOCK_CONVERSION_TABLE_NAME"),
                knock_conversion_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("KNOCK_CONVERSION_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
