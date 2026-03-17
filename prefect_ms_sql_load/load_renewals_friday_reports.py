import os
from dotenv import load_dotenv
from transform_data import RenewalsFridayReports
import logging
from database import MSSQL
from prefect import task

load_dotenv()

logging.basicConfig(
    filename="load_renewals_friday_reports.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)


@task(name="load_renewals_friday_reports")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables({os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME")})
    logging.info(
        f'Starting to stage data in the {os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME")} table'
    )
    friday_report_data = RenewalsFridayReports(
        os.getenv("FRIDAY_RENEWALS_REPORT_FILE_PATH")
    )
    friday_report_data.structurize_data()
    ms_sql.insert_dataframe(
        table_name=os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME"),
        df=friday_report_data.df,
    )
    logging.info(
        f'Sucessfully staged all data to {os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME")}'
    )
