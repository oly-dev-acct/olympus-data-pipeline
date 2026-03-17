from database import Snowflake
from datetime import datetime
import sys
import pandas as pd, logging, logging.handlers
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename='test_snowflake.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')



df=pd.read_excel(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"),sheet_name="Current Year")
print(df)
