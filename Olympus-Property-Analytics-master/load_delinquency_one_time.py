import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import pandas as pd
import time
import numpy as np
import re
from database import MSSQL
from transform_data import (
    DelinquentReports)
import glob


def structurize_excel_sheet_one(file):
    now = datetime.now()
    file_name = os.path.basename(file)
    xls = pd.ExcelFile(file)
    sheet_names = xls.sheet_names
    df = pd.read_excel(xls, sheet_name=sheet_names[0], nrows=1, dtype = str)
    count = 0
    if not df.empty:
        count = 1
        text = df.columns[0]
        pattern = r'FISCAL PERIOD (\d{6}) AS OF (\d{2}/\d{2}/\d{4})'
        match = re.search(pattern, text)
        date_as_of = None
        fiscal_period= None
        if match:
            fiscal_period = match.group(1)
            date_as_of = match.group(2)
    df = pd.read_excel(xls, sheet_name=sheet_names[0], skiprows = 1, dtype = str)
    if not df.empty and date_as_of is not None:
        totals_index = df[df.apply(lambda row: 'TOTALS' in row.values, axis=1)].index
        if not totals_index.empty:
            df = df.iloc[:totals_index[0]]
        df.rename(columns={'Bldg/\nUnit': 'unit_number',
                        '\nStatus': 'status',
                            'Move-In/\nOut': 'move_in_move_out',
                            'Code\nDescription': 'code_description',
                            'Total\nPrepaid': 'total_prepaid',
                            'Net\nBalance': 'net_balance',
                            '\nCurrent': 'current',
                            'Total\nDelinquent': 'total_delinquent',
                            '30\nDays': '30_days',
                            '60\nDays': '60_days',
                            '90+\nDays': '90_plus_days',
                            'Prorate  Credits ': 'prorate_credits',
                            'Deposits\nHeld': 'deposits_held',
                            'Outstanding\nDeposit': 'outstanding_deposit',
                            '# Late/\n# NSF': 'late_nsf',
                            'Unit': 'unit_number',
                            'Bldg/Unit': 'unit_number',
                            'Name': 'name',
                            'Phone Number': 'phone_number',
                            'Email': 'email',
                            'Status': 'status',
                            'Move-In/Out': 'move_in_move_out',
                            'Code Description': 'code_description',
                            'Total Prepaid': 'total_prepaid',
                            'Total Delinquent' : 'total_delinquent',
                            'Net Balance': 'net_balance',
                            'Current': 'current',
                            '30 Days': '30_days',
                            '60 Days': '60_days',
                            '90+ Days': '90_plus_days',
                            'Prorate Credit': 'prorate_credits',
                            'Deposits Held': 'deposits_held',
                            'Outstanding Deposit': 'outstanding_deposit'}
                ,inplace=True)
        if 'unit_number' not in df.columns:
            print("skipping this file")
            return df, 0
        df['move_in_move_out'] = df['move_in_move_out'].fillna('none')
        dates = df['move_in_move_out'].str.split('\n')
        for idx,i in enumerate(dates):
            len_of_list = len(i)
            if len_of_list != 2:
                dates[idx].append('none')
        move_in = []
        move_out = []
        for i in dates:
            move_in.append(i[0])
            move_out.append(i[1])
        df['move_in_date'] = move_in
        df['move_out_date'] = move_out
        df = df.dropna(how='all')
        df = df.dropna(axis=1, how='all')
        for index, row in df.iterrows():
            if pd.isna(row['unit_number']) and row['move_in_move_out'] != 'none':
                df.at[index - 1, 'move_out_date'] = row['move_in_move_out']
        
        #parts = file_name.split('-')
        #property_info = parts[0].split(' ')
        #print(property_info)
                
        '''
        df['property_number'] = property_info[0]
        property_name = property_info[1]
        if len(property_info) > 3:
            property_info = property_info[2:]
            for val in property_info:
                property_name = property_name + ' ' + val
        df['property_name'] = property_name'''
        df['property_name'] = file_name.split("_")[1]
        df["onsiteid"] = file_name.split("_")[0]
        df['fiscal_period'] = fiscal_period
        df['fiscal_as_of'] = date_as_of
  
        '''
        if len(property_info) > 3:
            property_info = property_info[2:]
            for val in property_info:
                property_name = property_name + ' ' + val
        df['property_name'] = property_name'''
        df['late_nsf'] = df.apply(lambda row: f"{int(row['#Late'])}/{int(row['#NSF'])}" if not pd.isna(row['#Late']) else pd.NA, axis=1)
        columns_to_drop=['#Late', '#NSF', 'D', 'O']
        
        for column_name in columns_to_drop:
            if column_name in df.columns:
                df.drop(columns=[column_name], inplace=True)
        columns_to_drop = [col for col in df.columns if col.startswith('Unnamed')]
        df.drop(columns=columns_to_drop, inplace=True)
        df = df.dropna(subset=['unit_number'])
        df = df.reset_index(drop=True)
        df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

        # only for sheet 2
        # if 'code_description' not in df.columns:
        #     position = df.columns.get_loc('move_in_move_out') + 1
        #     df.insert(position, 'code_description', np.nan)
        return df, count
    else:
        return df, count


def load_delinquent_files():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = ['delinquent_fiscal_end_load_stage_new']
    #ms_sql.truncate_tables(table_list=table_list)

    delinquent_folder = r'C:\Users\revenuemanagement\OneDrive - Olympus Property\Reports\PowerBIReportsFile\Reports\BI_delinquent_fiscal_month_end'
    start_time = time.time()

    for file in os.listdir(delinquent_folder):
        if file == '324 Odyssey Lake - 11.23 Delinquent and Prepaid - Excel.xls' or file == '411 Olympus Daybreak - 11.23 Delinquent and Prepaid - Excel.xls':
            continue
        file_path = os.path.join(delinquent_folder, file)
        #print(file_path)
        try:
            df,count = structurize_excel_sheet_one(file_path)
            if count == 1:
                ms_sql.insert_dataframe('delinquent_fiscal_end_load_stage', df)
        except Exception as e:
            print(f"Failed to process file: {file_path}")
            print(f"Error: {e}")
    elapsed_time = time.time() - start_time

def load_delinquent_files_normal_format():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = ['delinquent_fiscal_end_load_stage']
    #ms_sql.truncate_tables(table_list=table_list)

    delinquent_folder = r'C:\Users\revenuemanagement\OneDrive - Olympus Property\Reports\PowerBIReportsFile\Reports\BI_delinquent_save_off\23\Nov\not_processed'
    start_time = time.time()
    target_filename = '324 Odyssey Lake - 11.23 Delinquent and Prepaid - Excel.xls'

    for file in os.listdir(delinquent_folder):

        file_path = os.path.join(delinquent_folder, file)
        delinquent_data = DelinquentReports(file_path)
        count = delinquent_data.structurize_data()
        if count == 1:
            ms_sql.insert_dataframe('delinquent_fiscal_end_load_stage', delinquent_data.df)
    elapsed_time = time.time() - start_time
   


#filepath_name=r'C:\Users\revenuemanagement\OneDrive - Olympus Property\Reports\PowerBIReportsFile\Reports\BI_delinquent_save_off\23\Sep\3184421_Odyssey Lake _Delinquent and Prepaid_1.xls'
#print(structurize_excel_sheet_one(filepath_name))
load_delinquent_files()
#load_delinquent_files_normal_format()
