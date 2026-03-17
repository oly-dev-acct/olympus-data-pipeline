import os
from dotenv import load_dotenv
from transform_data import MaintainenceReports
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_requests_file.log",
    level=logging.DEBUG,  
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_request_files")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = [os.getenv("REQUESTS_TABLE_NAME")]
    ms_sql.truncate_tables(table_list=table_list)

    maintainence_folder = os.getenv("REQUESTS_FOLDER_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("REQUESTS_TABLE_NAME")} table'
    )
    start_time = time.time()
    maintainece_data = MaintainenceReports(maintainence_folder)
    maintainece_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("REQUESTS_TABLE_NAME"), maintainece_data.df)
    elapsed_time = (time.time() - start_time) / 60 
    logging.info(
        f'Sucessfully staged all data to {os.getenv("REQUESTS_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} minutes'
    )

    logging.info("The script was successfull")
