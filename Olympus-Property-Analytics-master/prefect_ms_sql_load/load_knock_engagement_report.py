import os
from dotenv import load_dotenv
from transform_data import KnockEngagementReportDaily
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_knock_engagement_report.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_knock_engagement_report")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME")} table'
    )
    directory = os.getenv("KNOCK_ENGAGEMENT_FOLDER_PATH")
    start_time = time.time()
    knock_engagement_report = KnockEngagementReportDaily(os.getenv("KNOCK_ENGAGEMENT_FOLDER_PATH"))
    knock_engagement_report.structurize_data()
    print(knock_engagement_report.df)
    ms_sql.insert_dataframe(
        os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME"),
        knock_engagement_report.df,
    )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
