import os
from dotenv import load_dotenv
from transform_data import UnitCapXReport, UnitRehabReport, UnitStandardTurnReport
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_unit_capx_rehab_standard_turn_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_unit_capx_rehab_standard_turn_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    table_list = [
        os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME"),
        os.getenv("UNIT_REHAB_REPORT_TABLE_NAME"),
        os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME"),
    ]
    ps_sql.truncate_tables(table_list=table_list)

    # load capx turn data to stage
    
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_turn_capx_report_data = UnitCapXReport(os.getenv("UNIT_TURN_CAPX_REPORT_FOLDER_PATH"))
    unit_turn_capx_report_data.structurize_data()
    ps_sql.insert_dataframe(os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME"), unit_turn_capx_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    # load rehab turn data to stage
    
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_REHAB_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_rehab_report_data = UnitRehabReport(os.getenv("UNIT_REHAB_REPORT_FOLDER_PATH"))
    unit_rehab_report_data.structurize_data()
    ps_sql.insert_dataframe(os.getenv("UNIT_REHAB_REPORT_TABLE_NAME"), unit_rehab_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_REHAB_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    # load standard turn data to stage

    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_standard_turn_report_data = UnitStandardTurnReport(os.getenv("UNIT_STANDARD_TURN_REPORT_FOLDER_PATH"))
    unit_standard_turn_report_data.structurize_data()
    ps_sql.insert_dataframe(os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME"), unit_standard_turn_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
