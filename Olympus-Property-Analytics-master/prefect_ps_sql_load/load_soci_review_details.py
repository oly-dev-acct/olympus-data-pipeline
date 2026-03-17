import os
from dotenv import load_dotenv
from transform_data import SOCIReviewFeed
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_soci_review_details_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_soci_review_details_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
        {
            os.getenv("SOCI_REVIEW_FEED_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("SOCI_REVIEW_FEED_TABLE_NAME")} table'
    )
    start_time = time.time()
    soci_review_feed_report_data = SOCIReviewFeed(os.getenv("SOCI_REVIEW_FEED_FOLDER_PATH"))
    soci_review_feed_report_data.structurize_data()

    ps_sql.insert_dataframe(os.getenv("SOCI_REVIEW_FEED_TABLE_NAME"), soci_review_feed_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SOCI_REVIEW_FEED_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
