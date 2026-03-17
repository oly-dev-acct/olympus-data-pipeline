import os
from dotenv import load_dotenv
from transform_data import DelinquentReportsNew
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_delinquent_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_delinquen_postgrest")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()

    table_list = [os.getenv("DELINQUENT_TABLE_NAME")]
    ps_sql.truncate_tables(table_list=table_list)

    delinquent_folder = os.getenv("DELINQUENT_FOLDER_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("DELINQUENT_TABLE_NAME")} table'
    )
    start_time = time.time()
    for file in os.listdir(delinquent_folder):
        if "zhistorical" in file.lower():
            continue
        file_path = os.path.join(delinquent_folder, file)
        delinquent_data = DelinquentReportsNew(file_path)
        delinquent_df,count = delinquent_data.structurize_data()
        #count = delinquent_data.structurize_data()
        if count == 1:
            #ps_sql.insert_dataframe(os.getenv("DELINQUENT_TABLE_NAME"), delinquent_data.df)
            ps_sql.insert_dataframe(os.getenv("DELINQUENT_TABLE_NAME"), delinquent_df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("DELINQUENT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
    logging.info("The script was successfull")
