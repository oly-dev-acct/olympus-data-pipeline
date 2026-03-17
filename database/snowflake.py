import snowflake.connector
import pandas as pd
import logging
import json
from uuid import getnode
from datetime import datetime
import os
from snowflake.connector.pandas_tools import write_pandas

now = datetime.now()
date_and_time_string = now.strftime("%d-%m-%Y--%H-%M-%S")
snow_flake_date_and_time_string = now.strftime("%d/%m/%Y %H:%M:%S")


class Snowflake:
    def __init__(self, user, password, account):
        self.connection = snowflake.connector.connect(
            user=user, password=password, account=account
        )
        self.cursor = self.connection.cursor()
        logging.info("Successfully connected to the Snowflake Database")

    def execute_query(self, query, param=None):
        self.cursor.execute(query, params=None)
        self.connection.commit()

    def execute_query_many(self, query, param):
        self.cursor.executemany(query, param)
        self.connection.commit()

    def snowflake_data_to_dataframe(self, sql):
        data = self.select_query(sql)
        df = pd.DataFrame(
            data,
            columns=[
                self.cursor.description[i][0]
                for i in range(len(self.cursor.description))
            ],
        )
        return df

    def select_query(self, query) -> list:
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def close_connection(self):
        self.connection.close()

