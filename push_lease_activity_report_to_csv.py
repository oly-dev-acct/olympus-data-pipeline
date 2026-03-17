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
        connection_string = f"mssql+pyodbc://CCPOLYSQL01\SQLEXPRESS/?database=Olympus_Property_Staging&driver={driver}&trusted_connection=yes"
        engine = create_engine(connection_string)
        #stored_procedure_name = 'sp_mirador_stovall_stat_report'
        query = text(f"""
                SELECT  [reportLeasingAgentID]
                ,[reportLeasingAgent]
                ,[contactDate]
                ,[contactTypeName]
                ,[ProspectName]
                ,[NewProspect]
                ,[InitialContactType]
                ,[TrafficOrigin]
                ,[GCFloorplanGroup]
                ,[GCFloorPlan]
                ,[DateNeeded]
                ,[NextScheduledFollowup]
                ,[pgcStatus]
                ,[leases]
                ,[waitlist]
                ,[netCancelDeny]
                ,[NotInRatio]
                ,[ResultsOrLostReason]
                ,[footnote1]
                ,[footnote2]
                ,[OneSiteid]
                ,[Property Name]
                FROM [Olympus_Property_Analytics].[dbo].[fact_lease_activity_detail]
                where contactDate > dateadd(month,-2,getdate())""")
        connection = engine.connect()
        result = connection.execute(query)
        rows = result.fetchall()
        connection.close()
        df = pd.DataFrame(rows, columns=result.keys())
        current_time = datetime.datetime.now()
        timestamp = str(current_time.strftime("%Y%m%d_%H%M%S"))
        file_name = r'''C:\Users\revenuemanagement\OneDrive - Olympus Property\Reports\PowerBIReportsFile\Reports\LeasingActivityDetail\lease_activity_detail_{}.csv'''.format(timestamp)
        df.to_csv(file_name, sep=',',index=False)

if __name__ == "__main__":
    get_stat_report()
