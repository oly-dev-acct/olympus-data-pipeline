import os
from dotenv import load_dotenv
from transform_data import LeaseActivityVisits
import logging
from database import MSSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_lease_activity_new.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_lease_activity_new")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("LEASE_ACTIVITY_NEW_TABLE_NAME"),
        }
    )
    directory = os.getenv("LEASE_ACTIVITY_NEW_FOLDER_PATH")

    start = time.time()
    logging.info(
        f'Starting to stage data in  {os.getenv("LEASE_ACTIVITY_NEW_TABLE_NAME")} tables'
    )

    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "activity".lower() in file.lower():
            try:
                lease_activity_visits = LeaseActivityVisits(full_path)
                lease_activity_visits.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("LEASE_ACTIVITY_NEW_TABLE_NAME"),
                    lease_activity_visits.df,
                )
            except Exception as e:
                logging.error("Script failed to run ", exc_info=e)
                #raise
            
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )   
