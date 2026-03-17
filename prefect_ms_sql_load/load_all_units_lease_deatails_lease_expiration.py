from prefect import task
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv
from transform_data import (
    LeaseExpirationData,
    LeaseDetailData,
    AllUnitsData,
    ResidentDemographicsData,
    LeaseActivityData,
    UnitSetupView
)
import logging
from database import MSSQL

load_dotenv()

logging.basicConfig(
    filename="load_all_units_lease_deatails_lease_expiration.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)


@task(name="load_all_units_lease_deatails_lease_expiration")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("ALL_UNITS_TABLE_NAME"),
            os.getenv("LEASE_DETAILS_TABLE_NAME"),
            os.getenv("LEASE_EXPIRATION_TABLE_NAME"),
            os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME"),
            os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
            os.getenv("UNIT_SETUP_VIEW_TABLE_NAME"),
        }
    )
    directory = os.getenv("MULTIPLE_PROPERTY_DATA")

    start = time.time()
    logging.info(
        f'Starting to stage data in the {os.getenv("ALL_UNITS_TABLE_NAME")}, {os.getenv("LEASE_DETAILS_TABLE_NAME")}, {os.getenv("LEASE_EXPIRATION_TABLE_NAME")}, {os.getenv("LEASE_ACTIVITY_TABLE_NAME")},{os.getenv("UNIT_SETUP_VIEW_TABLE_NAME")} and {os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME")} tables'
    )
    for file in os.listdir(directory):
        if "zhistorical" in file.lower():
            continue
        full_path = os.path.join(directory, file)
        try:
            if "all units".lower() in file.lower():
                all_units_data = AllUnitsData(full_path)
                all_units_data.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("ALL_UNITS_TABLE_NAME"), all_units_data.df
                )

            elif "lease details".lower() in file.lower():
                lease_details_data = LeaseDetailData(full_path)
                lease_details_data.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("LEASE_DETAILS_TABLE_NAME"), lease_details_data.df
                )

            elif "lease expiration renewal".lower() in file.lower():
                lease_expiration_data = LeaseExpirationData(full_path)
                lease_expiration_data.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("LEASE_EXPIRATION_TABLE_NAME"), lease_expiration_data.df
                )

            elif "resident demographics".lower() in file.lower():
                resident_demographics_data = ResidentDemographicsData(full_path)
                resident_demographics_data.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME"),
                    resident_demographics_data.df,
                )

            elif "leasing activity detail".lower() in file.lower():
                lease_activity_data = LeaseActivityData(full_path)
                lease_activity_data.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
                    lease_activity_data.df,
                )

            elif "unit setup view".lower() in file.lower():
                unit_setup_view = UnitSetupView(full_path)
                unit_setup_view.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("UNIT_SETUP_VIEW_TABLE_NAME"),
                    unit_setup_view.df,
                )

        except Exception as e:
            logging.error(f"Failed to process file '{file}': {str(e)}")
            continue
    
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("ALL_UNITS_TABLE_NAME")}, {os.getenv("LEASE_DETAILS_TABLE_NAME")}, {os.getenv("LEASE_EXPIRATION_TABLE_NAME")}, {os.getenv("LEASE_ACTIVITY_TABLE_NAME")},{os.getenv("UNIT_SETUP_VIEW_TABLE_NAME")} and {os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
