from database import MSSQL
import os
import glob
from dotenv import load_dotenv
import logging
from database import MSSQL
import time
from sqlalchemy import create_engine, text
import pandas as pd
import datetime


load_dotenv()
logging.basicConfig(
    filename="push_lease_activity_report_to_csv.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)  
    

def get_stat_report():
        driver='ODBC Driver 17 for SQL Server'
        connection_string = f"mssql+pyodbc://CCPOLYRM01\SQLEXPRESS/?database=Olympus_Property_Staging&driver={driver}&trusted_connection=yes"
        engine = create_engine(connection_string)
        #stored_procedure_name = 'sp_mirador_stovall_stat_report'
        query = text(f"""
    SELECT  [site_id_property_unit_number]
    ,[name]
    ,[lease_rent]
    ,[lease_start]
    ,[notes]
    ,[move_in]    
    FROM [Olympus_Property_Analytics].[dbo].[fact_lease_all_units]
    WHERE YEAR(lease_start) IN (2023, 2024)
    
""")

        connection = engine.connect()
        result = connection.execute(query)
        rows = result.fetchall()
        connection.close()
        df = pd.DataFrame(rows, columns=result.keys())
        current_time = datetime.datetime.now()
        timestamp = str(current_time.strftime("%Y%m%d_%H%M%S"))
        file_name = r'''C:\Users\revenuemanagement\OneDrive - Olympus Property\Reports\PowerBIReportsFile\Reports\LeasingActivityDetail\fact_lease_all_units_{}.csv'''.format(timestamp)
        df.to_csv(file_name, sep=',',index=False)

if __name__ == "__main__":
    get_stat_report()
