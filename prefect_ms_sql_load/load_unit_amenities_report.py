import os
from dotenv import load_dotenv
from transform_data import UnitAmenitiesReport
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_unit_amenities_report.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_unit_amenities_report")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_amenties_report_data = UnitAmenitiesReport(os.getenv("UNIT_AMENITIES_REPORT_FOLDER_PATH"))
    unit_amenties_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME"), unit_amenties_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
