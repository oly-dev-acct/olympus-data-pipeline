import os
from dotenv import load_dotenv
from transform_data import LeasetradereportData
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_lease_tradeout_report.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_lease_tradeout_report")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("LEASE_TRADE_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("LEASE_TRADE_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    lease_trade_report_data = LeasetradereportData(os.getenv("LEASE_TRADE_REPORT_FOLDER_PATH"))
    lease_trade_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("LEASE_TRADE_REPORT_TABLE_NAME"), lease_trade_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("LEASE_TRADE_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
    logging.info("The script was successfull")
