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
    WHERE YEAR(contactDate) IN (2023, 2024)
    AND OneSiteid IN (
        5222438, 5221064, 4404850, 5114990, 4950103, 5142569, 4830287, 
        4593607, 4950103, 5271241, 4650916, 4389612, 4728311, 4743027, 
        3184421, 4792391, 4792391, 4300780, 5101173, 4990772, 4904467, 
        4900324, 4533533, 4809399, 4381575, 4165264, 5302528, 5030884, 
        4786022, 4041690, 4041690, 4845868, 4054041, 4372463, 4960689, 
        4925178, 4966206, 4533527, 5181267, 4744577, 4430015, 4333293, 
        4115711, 4089538, 4369522, 5251673, 4905238, 4333747, 4852020, 
        5007191, 4333746, 4647580, 5101177, 4372464, 4948095, 4033816, 
        5344116, 4954360, 5060866, 4897049, 5211313, 4079287, 4632740,
        4237685, 4115712
    )
""")

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
