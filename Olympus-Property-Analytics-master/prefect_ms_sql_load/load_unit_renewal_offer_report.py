import os
from dotenv import load_dotenv
from transform_data import UnitRenewalOfferAnalysis
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_unit_renewal_offer_report.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_unit_renewal_offer_report")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_renewal_offer_data = UnitRenewalOfferAnalysis(os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_FOLDER_PATH"))
    unit_renewal_offer_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME"), unit_renewal_offer_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
