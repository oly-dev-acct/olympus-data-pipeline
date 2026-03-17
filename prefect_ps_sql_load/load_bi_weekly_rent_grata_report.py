import os
from dotenv import load_dotenv
from transform_data import RentGrataWeeklyReport
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_bi_weekly_rent_grata_report_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_bi_weekly_rent_grata_report_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
        {
            os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        bi_rent_grata_weekly_report = RentGrataWeeklyReport(full_path)
        bi_rent_grata_weekly_report.structurize_data()
        ps_sql.insert_dataframe(
            os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME"),
            bi_rent_grata_weekly_report.df,
        )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
