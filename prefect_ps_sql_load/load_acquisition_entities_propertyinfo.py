import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv
from transform_data import EntityData, AcquisitionData, PropertyInfoData
import logging
from database import PostgreSQL
from prefect import task, get_run_logger

load_dotenv()


logging.basicConfig(
    filename="load_acquisition_entities_propertyinfo_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)


@task(name="load_acquisition_entities_propertyinfo_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
        {
            os.getenv("PROPERTY_INFO_TABLE_NAME"),
            os.getenv("ENTITIES_TABLE_NAME"),
            os.getenv("AQUISITION_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("AQUISITION_TABLE_NAME")} table'
    )
    start_time = time.time()
    acquisition_data = AcquisitionData(os.getenv("AQUISITION_FILEPATH"))
    acquisition_data.structurize_data()
    ps_sql.insert_dataframe(os.getenv("AQUISITION_TABLE_NAME"), acquisition_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("AQUISITION_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info(
        f'Starting to stage data in the {os.getenv("ENTITIES_TABLE_NAME")} table'
    )
    start_time = time.time()
    entities_data = EntityData(os.getenv("ENTITIES_FILEPATH"))
    entities_data.structurize_data()
    ps_sql.insert_dataframe(os.getenv("ENTITIES_TABLE_NAME"), entities_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("ENTITIES_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info(
        f'Starting to stage data in the {os.getenv("PROPERTY_INFO_TABLE_NAME")} table'
    )
    start_time = time.time()
    property_info_data = PropertyInfoData(os.getenv("PROPERTY_INFO_FILEPATH"))
    property_info_data.structurize_data()
    ps_sql.insert_dataframe(
        os.getenv("PROPERTY_INFO_TABLE_NAME"), property_info_data.df
    )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("PROPERTY_INFO_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info("load_acquisition_entities_propertyinfo script was successfull")
