import os
from dotenv import load_dotenv
from transform_data import GoogleReviews
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_google_reviews.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_google_reviews")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("GOOGLE_REVIEW_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("GOOGLE_REVIEW_TABLE_NAME")} table'
    )
    start_time = time.time()
    google_review_data = GoogleReviews(os.getenv("GOOGLE_REVIEW_FOLDER_PATH"))
    google_review_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("GOOGLE_REVIEW_TABLE_NAME"), google_review_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("GOOGLE_REVIEW_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
    logging.info("The script was successfull")
