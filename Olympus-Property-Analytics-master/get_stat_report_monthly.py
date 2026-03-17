from database import MSSQL
import os
import glob
from dotenv import load_dotenv
import logging
from database import MSSQL
import time
from sqlalchemy import create_engine, text
import pandas as pd


load_dotenv()
logging.basicConfig(
    filename="get_stat_report_monthly.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

    
    

def get_stat_report():
        driver='ODBC Driver 17 for SQL Server'
        connection_string = f"mssql+pyodbc://CCPOLYSQL01\SQLEXPRESS/?database=Olympus_Property_Staging&driver={driver}&trusted_connection=yes"
        engine = create_engine(connection_string)
        stored_procedure_name = 'sp_mirador_stovall_stat_report'
        query = text(f" SET NOCOUNT ON;EXEC sp_mirador_stovall_stat_report")
        connection = engine.connect()
        result = connection.execute(query)
        rows = result.fetchall()
        connection.close()
        df = pd.DataFrame(rows, columns=result.keys())
        file_name=r'C:\Users\revenuemanagement\OneDrive - Olympus Property\Accounting\Investor Reports\LivCor\Stat Report\Stat_Report_Data_Mirador_and_Stovall\Mirador_and_Stovall_stat_report.csv'
        df = df.drop('property_number', axis=1)
        df.to_csv(file_name, sep=',',index=False)

if __name__ == "__main__":
    get_stat_report()
