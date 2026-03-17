import os
import glob
from dotenv import load_dotenv
from transform_data import UnitScheduledTransaction
import logging
from database import PostgreSQL
import time

logging.basicConfig(
    filename="load_unit_scheduled_transaction_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

if __name__ == "__main__":
    try:
        ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
        ps_sql.connect()

        unit_scheduled_transaction_files = glob.glob(
            os.path.join(os.getenv("MULTIPLE_PROPERTY_DATA"), "*Unit Scheduled Transactions*.xls")
        )

        table_list = [
            os.getenv("UNITSCHEDULEDTRANS_TABLE_NAME"),
        ]
        ps_sql.truncate_tables(table_list=table_list)

        logging.info(f'Starting to stage data in the {os.getenv("UNITSCHEDULEDTRANS_TABLE_NAME")} table')
        start_time = time.time()
        for unit_transaction_file in unit_scheduled_transaction_files:
            unit_transaction_data = UnitScheduledTransaction(unit_transaction_file)
            unit_transaction_data.structurize_data()
            ps_sql.insert_dataframe(
                os.getenv("UNITSCHEDULEDTRANS_TABLE_NAME"), unit_transaction_data.df
            )
        elapsed_time = (time.time() - start_time) / 60
        logging.info(f'Sucessfully staged all data to {os.getenv("AVAILABILITY_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} minutes')

        logging.info("The script was successfull")
    except Exception as e:
        logging.error("Script failed to run ", exc_info=e)
        exit()
