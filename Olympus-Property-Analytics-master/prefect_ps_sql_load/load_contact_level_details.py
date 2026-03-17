import os
from datetime import datetime, date,timedelta
from prefect_ps_sql_load.variable import *
import pandas as pd
from dotenv import load_dotenv
from transform_data import AvailabiltyData, TurnoverData
import logging
from database import PostgreSQL
from prefect import task
import glob
import time


logging.basicConfig(
    filename="load_contact_level_details.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()


class ContactLevelDetailsLoader:
    def __init__(
        self,
        folder_path,
        schema,
        db_params,
        table_name,
        postgres_class
    ):
        """
        Initialize loader with configuration.
        """
        self.folder_path = folder_path
        self.schema = schema
        self.db_params = db_params
        self.table_name = table_name
        self.postgres_class = postgres_class

    # ------------------ FILE DISCOVERY ------------------

    def get_today_files(self):
        """
        Return CSV files modified today containing 'contact level details' in filename.
        """
        today_files = []
        for file_name in os.listdir(self.folder_path):
            if file_name.lower().endswith(".csv") and "contact level details" in file_name.lower():
                file_path = os.path.join(self.folder_path, file_name)
                modified_date = datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                ).date()
                if modified_date == date.today():
                    today_files.append(file_name)
        return today_files

    # ------------------ METADATA ------------------

    @staticmethod
    def extract_metadata_from_filename(file_name):
        """
        Extract onsiteid and property_name from filename.
        Expected format: <onsiteid>_<property_name>_...
        """
        base_name = file_name.replace(".csv", "")
        parts = base_name.split("_")
        if len(parts) < 2:
            return None, None
        onsiteid = parts[0]
        property_name = parts[1].strip()
        return onsiteid, property_name

    # ------------------ FILE READING ------------------

    def read_contact_csv(self, file_path, onsiteid, property_name):
        """
        Read CSV safely, skip junk rows with extra columns,
        and also collect them in a bad_rows DataFrame.
        """
        bad_rows = []

        def bad_line_handler(bad_line):
            # bad_line is a list representing the junk row
            bad_rows.append(bad_line)
            return None  # tells pandas to skip the row

        try:
            df = pd.read_csv(
                file_path,
                usecols=list(self.schema.keys()),  # expected columns only
                engine="python",                   # REQUIRED
                on_bad_lines=bad_line_handler,     # capture junk rows
                dtype=str
            )

            df.rename(columns=self.schema, inplace=True)

            df["onsiteid"] = onsiteid
            df["property_name"] = property_name
            df["load_timestamp"] = pd.Timestamp.now()

            df = df.replace(r'^\s*$', None, regex=True)

            # Create bad rows DataFrame (optional use)
            bad_df = pd.DataFrame(bad_rows)
            if not bad_df.empty:
                bad_df["source_file"] = os.path.basename(file_path)
                bad_df["load_timestamp"] = pd.Timestamp.now()

            return df, bad_df

        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return None, None


    # ------------------ TRANSFORMATION ------------------

    def process_all_files(self):
        """
        Process all valid CSV files and return a combined DataFrame.
        """
        all_dfs = []
        files = self.get_today_files()
        print("List of files:", files)

        for file_name in files:
            onsiteid, property_name = self.extract_metadata_from_filename(file_name)
            if not onsiteid or not property_name:
                print(f"Skipping file with unexpected filename format: {file_name}")
                continue

            file_path = os.path.join(self.folder_path, file_name)
            df,bad_df  = self.read_contact_csv(file_path, onsiteid, property_name)

            if df is not None:
                all_dfs.append(df)

        # print("bad data is",bad_df)
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            print("Combined DataFrame shape:", combined_df.shape)
            return combined_df

        print("No valid files to combine.")
        return pd.DataFrame()

    # ------------------ LOAD ------------------

    def load_to_postgres(self, df):
        """
        Truncate table and load data to PostgreSQL.
        """
        if df.empty:
            print("No data to load.")
            return

        pg = self.postgres_class(**self.db_params)
        pg.connect()

        try:
            pg.truncate_tables([self.table_name])
            print("Table truncated")

            pg.insert_dataframe_copy_command_contact_details(
                self.table_name,
                df
            )
            print(f"Data inserted successfully into {self.table_name}")

        except Exception as e:
            print(f"Failed to insert data: {e}")

    # ------------------ MAIN ENTRY POINT ------------------

    def run(self):
        """
        Equivalent to your previous main() function.
        """
        final_df = self.process_all_files()
        self.load_to_postgres(final_df)
'''
#######if __name__ == "__main__":
    #############uncomment below class call before running the class
   loader = ContactLevelDetailsLoader(
        folder_path=MULTIPLE_PROPERTY_DATA,
        schema=SCHEMA,
        db_params=DB_PARAMS,
        table_name=CONTACT_LEVEL_DETAILS,
        postgres_class=PostgreSQL
    )

    # This executes the full transformation + load
    loader.run()
    '''


@task(name="load_contact_level_details")
def main():
    
    loader = ContactLevelDetailsLoader(
        folder_path= os.getenv("MULTIPLE_PROPERTY_DATA") ,
        schema=SCHEMA,
        db_params={"host":os.getenv("PS_SERVER_NAME"), 
         "database":os.getenv("PS_DATABASE_NAME"),
         "user":os.getenv("PS_USER_NAME"), 
         "password":os.getenv("PS_PASSWORD"),
         "port": 5432,
         "schema":os.getenv("PS_SCHEMA")},
        table_name=os.getenv("CONTACT_LEVEL_DETAILS"),
        postgres_class=PostgreSQL
    )

    # This executes the full transformation + load
    loader.run()

    logging.info("load_contact_level_details script executed successfully")
