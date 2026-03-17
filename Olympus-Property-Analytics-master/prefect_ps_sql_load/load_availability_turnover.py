import os
import glob
from dotenv import load_dotenv
from transform_data import AvailabiltyData, TurnoverData
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_availability_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()


@task(name="load_availability_turnover_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()

    availability_files = glob.glob(
        os.path.join(os.getenv("MULTIPLE_PROPERTY_DATA"), "*Availability*.xls")
    )
    turnover_files = glob.glob(
        os.path.join(os.getenv("MULTIPLE_PROPERTY_DATA"), "*turnover*.xls")
    )

    availability_files = [
    f for f in availability_files if "zhistorical" not in f.lower()
    ]

    turnover_files = [
    f for f in turnover_files if "zhistorical" not in f.lower()
    ]

    table_list = [
        os.getenv("AVAILABILITY_TABLE_NAME"),
        os.getenv("TURNOVER_TABLE_NAME"),
    ]
    ps_sql.truncate_tables(table_list=table_list)

    logging.info(
        f'Starting to stage data in the {os.getenv("AVAILABILITY_TABLE_NAME")} table'
    )
    start_time = time.time()
    for availabilty_file in availability_files:
        availability_data = AvailabiltyData(availabilty_file)
        availability_data.structurize_data()
        if not availability_data.df is None:
            ps_sql.insert_dataframe(
                os.getenv("AVAILABILITY_TABLE_NAME"), availability_data.df
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("AVAILABILITY_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info(
        f'Starting to stage data in the {os.getenv("TURNOVER_TABLE_NAME")} table'
    )
    start_time = time.time()
   
    for turnover_file in turnover_files:
        turnover_data = TurnoverData(turnover_file)
        turnover_data.structurize_data()
        if not turnover_data.df is None:
            ps_sql.insert_dataframe(os.getenv("TURNOVER_TABLE_NAME"), turnover_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("TURNOVER_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info("load_availability_turnover script was successfull")
