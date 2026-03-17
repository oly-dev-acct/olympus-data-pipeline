import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv
from transform_data import IncomeBudgetData
import logging
from database import MSSQL
from prefect import task

load_dotenv()
logging.basicConfig(
    filename="process_finance_statements_to_sql.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger()


def income_statement(ms_sql):
    logger.info("started working on income statement")
    income_data = IncomeBudgetData(os.getenv("INCOME_DATA_FILEPATH"))
    income_data.structurize_data()
    ms_sql.insert_dataframe(
        table_name=os.getenv("INCOME_STAGGING_TABLE_NAME"), df=income_data.df
    )


def budget_statement(ms_sql):
    logger.info("started working on budeget statement")
    budget_data = IncomeBudgetData(os.getenv("BUDGET_DATA_FILEPATH"))
    budget_data.structurize_data()
    ms_sql.insert_dataframe(
        table_name=os.getenv("BUDGET_STAGGING_TABLE_NAME"), df=budget_data.df
    )


@task(name="process_finance_statements_to_sql")
def main():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BUDGET_STAGGING_TABLE_NAME"),
            os.getenv("INCOME_STAGGING_TABLE_NAME"),
        }
    )
    income_statement(ms_sql)
    budget_statement(ms_sql)
    logging.info("process_finance_statements_to_sql script was successful")
