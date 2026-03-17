import pandas as pd
from datetime import datetime
import logging
import numpy as np
import os
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import re

load_dotenv()

now = datetime.now()


class Data:
    def __init__(self, file):
        self.file = file
        self.df = None

    def structurize_data(self):
        raise NotImplementedError(
            "structurize_data() method needs to be implemented in the subclasses."
        )

    def read_csv_file(self, skiprow=None, header="infer", dtype=object):
        return pd.read_csv(
            self.file, low_memory=False, skiprows=skiprow, header=header, dtype=dtype
        )

    def read_csv_file_without_header(self, skiprow=None, header="infer", dtype=object):
        return pd.read_csv(
            self.file, low_memory=False, skiprows=skiprow, header=None, dtype=dtype
        )

    def read_quote_csv_file(self, skiprow=None, header="infer", dtype=object):
        return pd.read_csv(
            self.file, quotechar='"', low_memory=False, skiprows=skiprow, header=header, dtype=dtype
        )

    def read_csv_file_tab_delimeted(self, skiprow=None, header="infer", dtype=object):
        return pd.read_csv(
            self.file,delimiter='\t',low_memory=False, skiprows=skiprow, header=header, dtype=dtype
        )

    def read_excel_file(self, sheet_name=0, skiprow=None, header=0, dtype=object):
        return pd.read_excel(
            self.file,
            sheet_name=sheet_name,
            skiprows=skiprow,
            header=header,
            dtype=dtype
        )
    def add_custom_compaign_column(self,row):
        if "Lead" in row:
            return "Lead Conversion"
        elif "Move-In" in row:
            return "Move In"
        elif "Move-Out" in row:
            return "Move Out"
        elif "Pre Renewal" in row:
            return "Pre Renewal"
        elif "Maintenance" in row:
            return "Maintenance"
        else:
            return None

    def replace_nan_values(self, df):
        return df.replace(np.nan, None)

    def convert_to_string(self, df):
        return df.astype(str)

    def read_google_csv(self):
        with open(self.file, 'r') as csv_file:
            input_rows = [line.strip() for line in csv_file.readlines()]
            merged_rows = [f"{input_rows[i]},{input_rows[i + 1]}" for i in range(0, len(input_rows), 2)]
            split_results = [result.split(',') for result in merged_rows]
            result_df = pd.DataFrame(split_results, columns=['Property_Name', 'ReviewDate','Google_Stars','Google_Reviews','Blank'])
        return result_df

    def read_knock_conversion_csv(self):
        with open(self.file, 'r') as csv_file:
            lines = csv_file.readlines()
        headers = [line.strip() for line in lines[:8]]
        data = []
        for i in range(8, len(lines), 8):
            row = [line.strip() for line in lines[i:i+8]]
            data.append(row)
        df = pd.DataFrame(data, columns=headers)
        return df

    def ready_activity_report_xml(self):
        tree = ET.parse(self.file)
        root = tree.getroot()
        data_list = []
        #Extract file-level information
        file_id = root.find('.//FileID').text
        file_date = root.find('.//FileDate').text

        first_row = root.find('.//Row')
        if first_row is not None:
            pmc_name = first_row.attrib.get('PMCName', '')
            pmc_id = first_row.attrib.get('pmcId', '')
            siteName = first_row.attrib.get('siteName', '')
            siteId = first_row.attrib.get('siteId', '')
        #Iterate through each <Row> element and extract relevant information
        for row in root.findall('.//Row'):

            row_dict = {
                'FileID': file_id,
                'FileDate': file_date,
                'PMCName': pmc_name,
                'pmcId': pmc_id,
                'siteName': siteName,
                'siteId': siteId,
                'reportLeasingAgentID': row.attrib.get('reportLeasingAgentID', ''),
                'reportLeasingAgent': row.attrib.get('reportLeasingAgent', ''),
                'contactDate': row.attrib.get('contactDate', ''),
                'contactTypeName': row.attrib.get('contactTypeName', ''),
                'ProspectName': row.attrib.get('ProspectName', ''),
                'NewProspect': row.attrib.get('NewProspect', ''),
                'InitialContactType': row.attrib.get('InitialContactType', ''),
                'TrafficOrigin': row.attrib.get('TrafficOrigin', ''),
                'GCFloorplanGroup': row.attrib.get('GCFloorplanGroup', ''),
                'GCFloorPlan': row.attrib.get('GCFloorPlan', ''),
                'DateNeeded': row.attrib.get('DateNeeded', ''),
                'NextScheduledFollowup': row.attrib.get('NextScheduledFollowup', ''),
                'pgcStatus': row.attrib.get('pgcStatus', ''),
                'leases': row.attrib.get('leases', ''),
                'waitlist': row.attrib.get('waitlist', ''),
                'netCancelDeny': row.attrib.get('netCancelDeny', ''),
                'NotInRatio': row.attrib.get('NotInRatio', ''),
                'ResultsOrLostReason': row.attrib.get('ResultsOrLostReason', ''),
                'footnote1': row.attrib.get('footnote1', ''),
                'footnote2': row.attrib.get('footnote2', '')
            }
            data_list.append(row_dict)
        
        #Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(data_list)
        return df


class IncomeBudgetData(Data):
    def structurize_data(self):
        df = self.read_csv_file(skiprow=2, header=None)

        df = df[
            ~df.apply(
                lambda row: row.astype(str).str.contains("Month Ending", case=False)
            ).any(axis=1)
        ]
        df = df[
            ~df.apply(
                lambda row: row.astype(str).str.contains("Actual", case=False)
            ).any(axis=1)
        ]
        df = df[
            ~df.apply(
                lambda row: row.astype(str).str.contains("Budget", case=True)
            ).any(axis=1)
        ]

        df = df.transpose()
        df.at[0, 0] = "propertyName"
        df.at[0, 2] = "month_end_date"
        df.columns = df.iloc[0]
        df = df[1:]
        unpivotlist = df.columns[2:].tolist()
        df = df.melt(
            id_vars=["propertyName", "month_end_date"],
            value_vars=unpivotlist,
            var_name="category_name",
            value_name="category_value",
            ignore_index=True,
        )
        df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")  #'%Y-%m-%d %H:%M:%S'

        df = df[
            ~df.apply(
                lambda row: row.astype(str).str.contains("12 Month", case=False)
            ).any(axis=1)
        ]

        df = df[
            ~df.apply(
                lambda row: row.astype(str).str.contains("All Locations", case=False)
            ).any(axis=1)
        ]

        logging.info("file data has been structured successfully")
        self.df = df


class EntityData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file()
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class AcquisitionData(Data):
    def structurize_data(self):
        self.df = self.read_excel_file()
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class PropertyInfoData(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="RevenueManagementPropertyInfo")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class LeaseDetailData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)

        strings_in_filepath = (os.path.basename(self.file)).split("_")
        self.df["On Site id"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class LeaseExpirationData(Data):
    def structurize_data(self):
        df_s2 = self.read_excel_file(sheet_name="Lease Expiration Detail", skiprow=1)
        df_s3 = self.read_excel_file(sheet_name="Renewal Detail", skiprow=1)

        first_row_with_all_NaN = df_s2[
            df_s2.isnull().all(axis=1) == True
        ].index.tolist()[0]
        df_s2 = df_s2.loc[0 : first_row_with_all_NaN - 1]
        df_s2 = df_s2.loc[:, ~df_s2.columns.str.startswith("Unnamed")]

        first_row_with_all_NaN = df_s3[
            df_s3.isnull().all(axis=1) == True
        ].index.tolist()[0]
        df_s3 = df_s3.loc[0 : first_row_with_all_NaN - 1]
        df_s3 = df_s3.loc[:, ~df_s3.columns.str.startswith("Unnamed")]

        self.df = pd.concat([df_s2, df_s3])
        strings_in_filepath = (os.path.basename(self.file)).split("_")
        self.df["On Site id"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class AllUnitsData(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(skiprow=6, sheet_name="Sheet1")

        first_row_with_all_NaN = self.df[
            self.df.isnull().all(axis=1) == True
        ].index.tolist()[0]
        self.df = self.df.loc[0 : first_row_with_all_NaN - 1]
        self.df = self.df.loc[:, ~self.df.columns.str.startswith("Unnamed")]

        strings_in_filepath = (os.path.basename(self.file)).split("_")

        self.df["On Site id"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class AvailabiltyData(Data):
    def structurize_data(self):
        df = pd.read_excel(self.file, skiprows=6, dtype={"Bldg/\nUnit": str})
        breaks_index = df[df["Unnamed: 0"].str.startswith("BREAKS", na=False)].index
        if not breaks_index.empty:
            df = df.iloc[: breaks_index[0]]
        breaks_index_sec = df[df["Unnamed: 0"].str.startswith("SKIPS", na=False)].index
        if not breaks_index_sec.empty:
            df = df.iloc[: breaks_index_sec[0]]
        breaks_index_third = df[df["Unnamed: 0"].str.contains("This unit is on hold", na=False)].index
        if not breaks_index_third.empty:
            df = df.iloc[: breaks_index_third[0]]
        df["Type"] = df["Unnamed: 0"].str.split("(").str[0].str.strip()
        df["Type"] = df["Type"].replace(["BREAK", "SKIP"], float("nan"))
        df["Type"] = df["Type"].ffill()
        df["BreakOrSkip"] = df["Unnamed: 0"]
        df.dropna(how='all', inplace=True)
        if not df.empty:
            row = df.iloc[1]
            for col in row.index:
                if pd.notnull(row[col]):
                    df.rename(columns={col: row[col]}, inplace=True)
            df = df.rename(columns=lambda x: x.replace("\n", " "))
            df = df[~df["Bldg/ Unit"].isna()]
            df.drop("Unnamed: 0", axis=1, inplace=True)
            for col in df.columns:
                if col.startswith("Unnamed") and df[col].isna().all():
                    df.drop(columns=col, inplace=True)
            for index, col in enumerate(df.columns):
                if col.startswith("Unnamed"):
                    previous_column = df.columns[index - 1]
                    df[previous_column] = df[col]
            df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
            file_name = os.path.basename(self.file)
            df["OnSiteId"] = file_name.split("_")[0]
            df["Property Name"] = file_name.split("_")[1]
            df["SiteUnitId"] = (
                df["OnSiteId"].astype(str) + "-" + df["Bldg/ Unit"].astype(str)
            )
            df["Days Vacant"] = df["Days Vacant"].astype(str)
            df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self.df = df


class TurnoverData(Data):
    def structurize_data(self):
        df = pd.read_excel(self.file, skiprows=11, dtype={"Unit #": str})
        if not df.empty:
            df = df.rename(columns=lambda x: x.replace("\n", " "))
            df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
            df = df.dropna(how="all")
            df = df[~(df["# of bedrooms"].isna() | df["# of bedrooms"].eq("# of bedrooms"))]
            file_name = os.path.basename(self.file)
            df["OnSiteId"] = file_name.split("_")[0]
            df["Property Name"] = file_name.split("_")[1]
            df["SiteUnitDateId"] = (
                df["OnSiteId"].astype(str)
                + "-"
                + df["Unit #"].astype(str)
                + "-"
                + df["Move-out date"].astype(str)
            )
            df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self.df = df


class ResidentDemographicsData(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(skiprow=8, sheet_name="Sheet1")
        if not self.df.empty:
            if self.df.columns[0] == "Unnamed: 0":
                self.df = self.read_excel_file(skiprow=9, sheet_name="Sheet1")
            self.df.dropna(how="all", inplace=True)
            self.df = self.df.loc[:, ~self.df.columns.str.startswith("Unnamed")]
            self.df.columns = self.df.columns.str.replace(" ", "")
            self.df.columns = self.df.columns.str.replace("\n", "")
            self.df[["Name", "Phone"]] = self.df["Name/Phone"].str.split(
                "\n|\r", expand=True
            )
            self.df = self.df.drop("Name/Phone", axis=1)
            contains_newline = self.df["BirthDate/Age"].str.contains("\n")
            split_result_birthday_age = self.df["BirthDate/Age"].str.split("\n", n=1, expand=True)
            if len(split_result_birthday_age.columns) == 2:
                self.df[["Birth Date", "Age"]] = self.df["BirthDate/Age"].str.split(
                "\n",n=1, expand=True)
            if len(split_result_birthday_age.columns) == 1:
                self.df["Birth Date"]=self.df["BirthDate/Age"]
                self.df["Age"]="None"
            
            self.df = self.df.drop("BirthDate/Age", axis=1)

            self.df[["Household Status", "Signer Status"]] = self.df[
                "HouseholdStatus/SignerStatus"
            ].str.split("\n", expand=True)
            self.df = self.df.drop("HouseholdStatus/SignerStatus", axis=1)
            split_result = self.df["Employer/JobType"].str.split("\n", n=1, expand=True)


            if len(split_result.columns) == 2:
                self.df[["Employer","Job Type"]] = self.df["Employer/JobType"].str.split(
               "\n", expand=True
            )
            if len(split_result.columns) == 1:
                self.df["Employer"]=self.df["Employer/JobType"]
                self.df["Job Type"]="None"
            self.df = self.df.drop("Employer/JobType", axis=1)

            if "Bldg/Unit" in self.df.columns:
                self.df = self.df.rename(columns={"Bldg/Unit": "Unit"})

            self.df = self.df.replace("", None)

            strings_in_filepath = (os.path.basename(self.file)).split("_")
            self.df["On Site id"] = strings_in_filepath[0]
            self.df["Property Name"] = strings_in_filepath[1]
            self.df = self.replace_nan_values(self.df)
            self.df = self.convert_to_string(self.df)
            self.df["Unit"] = self.df["Unit"].astype(str).replace("\.0", "", regex=True)
            self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class WeeklyReports(Data):
    def structurize_data(self):
        df = pd.read_excel(self.file)
        count = 0
        if not df.empty:
            count = 1
            date_column = df.iloc[2].first_valid_index()
            value = df.iloc[2][date_column]
            date_str = value.split("-")[-1].strip()
            date = date_str.split()[-1]
            date_parts = date.split("/")
            formatted_date = f"{int(date_parts[0]):02d}-{int(date_parts[1]):02d}-{date_parts[2]}"
            df = pd.read_excel(self.file, skiprows=10)
            df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
            df = df[~df["Property"].isna()]
            df = df.rename(
                columns={
                    "Total": "Total Property Visits",
                    "1st Visit": "1st Property Visit",
                    "Return Visit": "Return Property Visit",
                    "Applied": "Leasing Activity Applied",
                    "MI": "Occupancy MI",
                    "MO": "Occupancy MO",
                }
            )
            df["WeekDate"] = formatted_date
            df = df[df["Property"] != "Total"]
            df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self.df = df
            return count
        else:
            self.df = df
            return count


class UnitScheduledTransaction(Data):
    def structurize_data(self):
        df = pd.read_excel(self.file, skiprows=12, dtype=object)
        df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
        df = df.dropna(how="all")
        df = df.rename(columns=lambda x: x.replace("\n", " "))
        df = df.rename(
            columns={" Bldg/Unit": "Bldg/Unit", " SQFT": "SQFT", " Other": "Other"}
        )
        df = df[~df["Code/ Amenity Name"].isna()]
        df["Bldg/Unit"] = df["Bldg/Unit"].ffill()
        df["Floor Plan Code"] = df["Floor Plan Code"].ffill()
        df["Floor Plan Name"] = df["Floor Plan Name"].ffill()
        df["SQFT"] = df["SQFT"].ffill()
        file_name = os.path.basename(self.file)
        df["OnSiteId"] = file_name.split("_")[0]
        df["Property Name"] = file_name.split("_")[1]
        self.df = df


class RenewalsFridayReports(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="RenewalsFridayReports")
        self.df = self.df.drop("Custom.Data.Column2", axis=1)
        self.df["Bldg/Unit"] = self.df["Bldg/Unit"].str.replace(" ", "")
        self.df = self.df.rename(
            columns={"Property#": "On Site id", "CNS": "Leasing Consultant"}
        )
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)

class MaintainenceReports(Data):
    def structurize_data(self):
        list_of_dicts = []
        for file in os.listdir(self.file):
            file_path = os.path.join(self.file, file)
            df = pd.read_excel(file_path, dtype=str)
            drop_index = df[df['Unnamed: 0'] == "Request number:"]
            if len(drop_index) != 0:
                file_name = os.path.basename(file_path)
                onsiteid = file_name.split("_")[0]
                propertyname = file_name.split("_")[1]
                drop_index = drop_index.index[0]
                df = df.iloc[drop_index:]
                df.dropna(axis=0, how='all', inplace =True)
                df.dropna(axis=1, how='all', inplace = True)
                df.reset_index(drop=True, inplace=True)
                row_to_insert = []
                for index, row in df.iterrows():
                    if row[0] == 'Request number:':
                        row_to_insert.extend([["New Row Started"]])
                    row_to_insert.append(row.tolist())
                cleaned_row_to_insert = []
                for sublist in row_to_insert:
                    cleaned_sublist = [item for item in sublist if not pd.isna(item)]
                    cleaned_row_to_insert.append(cleaned_sublist)
                row_to_insert = cleaned_row_to_insert
                repition_cleaned = []
                previous_item = None
                for item in row_to_insert:
                    if (item[0] == 'Parts used' and item == previous_item) or (item[0] == 'Total Cost' and item == previous_item):
                        previous_item = item
                        continue
                    else:
                        repition_cleaned.append(item)
                    previous_item = item
                row_to_insert = repition_cleaned
                for index,row in enumerate(row_to_insert):
                    lists_to_drop = []
                    if row[0] == 'Parts used' and (index == len(row_to_insert)-1):
                        row.insert(1, 'none')
                        row.insert(3, 'none')
                    elif row[0] == 'Parts used' and row_to_insert[index+1][0] == 'Total Cost':
                        row.insert(1, 'none')
                        row.insert(3, 'none')
                        row_to_insert[index+1].insert(1, 'none')
                    elif row[0] == 'Time worked by:' and (row_to_insert[index-1][0] != 'Assigned to:' and row_to_insert[index-1][0] != 'Service action:'):
                        lists_to_drop.append(row_to_insert[index-1])
                        row.insert(1, row_to_insert[index-1][0])
                        if len(row_to_insert[index-1]) == 2:
                            row.append(row_to_insert[index-1][1])
                        else:
                            row.append('none')
                        for list_drop in lists_to_drop:
                            row_to_insert.remove(list_drop)
                for idx ,item in enumerate(row_to_insert):
                    drop_list_collector = []
                    if len(item) == 2 and item[0] == 'Total Cost' and len(row_to_insert[idx-1]) ==1 and row_to_insert[idx-1][0] =='Total Cost':
                        drop_list_collector.append(row_to_insert[idx-1])
                        if (idx+1) <= (len(row_to_insert) - 1):
                            if len(row_to_insert[idx+1]) ==1 and row_to_insert[idx+1][0] =='Total Cost':
                                drop_list_collector.append(row_to_insert[idx+1])
                    for drop_list in drop_list_collector:
                        row_to_insert.remove(drop_list)
                row_to_insert = row_to_insert[1:]
                for row in row_to_insert:
                    if row[0] == 'Request number:':
                        if row[1] == 'Status:':
                            row.insert(1, 'none')
                        if row[-1] == 'Status:':
                            row.append('none')
                        row[0] = 'request_number'
                        row[2] = 'status'
                    elif row[0] == 'Location:':
                        if row[1] == 'Created date/time:':
                            row.insert(1, 'none')
                        if row[-1] == 'Created date/time:':
                            row.append('none')
                        row[0] = 'location'
                        row[2] = 'created_date_time'
                    elif row[0] == 'Requestor:':
                        if row[1] == 'Completed date/time:':
                            row.insert(1, 'none')
                        if row[-1] == 'Completed date/time:':
                            row.append('none')
                        row[0] = 'requestor'
                        row[2] = 'completed_date_time'
                    elif row[0] == 'Assigned to:':
                        if row[-1] == 'Assigned to:':
                            row.append('none')
                        row[0] = 'assigned_to'
                    elif row[0] == 'Time worked by:':
                        if row[1] == 'Work started date/time:':
                            row.insert(1, 'none')
                        if row[-1] == 'Work started date/time:':
                            row.append('none')
                        row[0] = 'time_worked_by'
                        row[2] = 'work_started_date_time'
                    elif row[0] == 'Category:':
                        if row[1] == 'Work ended date/time:':
                            row.insert(1, 'none')
                        if row[-1] == 'Work ended date/time:':
                            row.append('none')  
                        row[0] = 'category'
                        row[2] = 'work_ended_date_time'
                    elif row[0] == 'Service issue:':
                        if row[1] == 'Time worked:':
                            row.insert(1, 'none')
                        if row[-1] == 'Time worked:':
                            row.append('none')  
                        row[0] = 'service_issue'
                        row[2] = 'time_worked'
                    elif row[0] == 'Service action:':
                        if row[-1] == 'Service action:':
                            row.append('none')
                        row[0] = 'service_action'
                    elif row[0] == 'Parts used':
                        row[0] = 'parts_used'
                        if row[1] == 'none':
                            row[2] = 'parts_cost'
                        else:
                            row[1] = 'parts_cost'
                if row_to_insert[-1] != ['New Row Started']:
                    row_to_insert.append(['New Row Started'])
                # for row in row_to_insert:
                #     if row[0] == 'time_worked_by':
                #         if row[3] != 'none':
                #             row[3] = row[3].strftime('%Y-%m-%d %H:%M:%S')
                #     if row[0] == 'category:':
                #         if row[3] != 'none':
                #             row[3] = row[3].strftime('%Y-%m-%d %H:%M:%S')
                dict_to_insert = {}
                worked_by_list_of_dicts = []
                worked_by_dict_to_insert = {}
                parts_list_of_dicts = []
                indexes_to_skip = []
                for idx, row in enumerate(row_to_insert):
                    break_count = 0
                    if idx in indexes_to_skip:
                        continue
                    elif row != ['New Row Started']:
                        if row[0] == 'time_worked_by':
                            key_value_pairs = [(row[i], row[i+1]) for i in range(0, len(row), 2)]
                            worked_by_dict_to_insert.update(key_value_pairs)
                            count = 1
                            while row_to_insert[idx+count][0] != 'parts_used':
                                if row_to_insert[idx+count][0] != 'time_worked_by':
                                    if row_to_insert[idx+count] == ['New Row Started']: #special condition for 3720740
                                        dict_to_insert.update({'Property': propertyname, 'Onsiteid': onsiteid})
                                        list_of_dicts.append(dict_to_insert)
                                        worked_by_list_of_dicts.append(worked_by_dict_to_insert)
                                        dict_to_insert.update({'worked_by': worked_by_list_of_dicts})
                                        dict_to_insert = {}
                                        worked_by_dict_to_insert = {}
                                        worked_by_list_of_dicts = []
                                        parts_list_of_dicts = []
                                        break_count = 1
                                        break
                                    key_value_pairs = [(row_to_insert[idx+count][i], row_to_insert[idx+count][i+1]) for i in range(0, len(row_to_insert[idx+count]), 2)]
                                    worked_by_dict_to_insert.update(key_value_pairs)
                                    indexes_to_skip.append(idx+count)
                                    count += 1
                                elif row_to_insert[idx+count][0] == 'time_worked_by':
                                    worked_by_list_of_dicts.append(worked_by_dict_to_insert)
                                    worked_by_dict_to_insert = {}
                                    key_value_pairs = [(row_to_insert[idx+count][i], row_to_insert[idx+count][i+1]) for i in range(0, len(row_to_insert[idx+count]), 2)]
                                    worked_by_dict_to_insert.update(key_value_pairs)
                                    indexes_to_skip.append(idx+count)
                                    count += 1
                            if break_count == 1:
                                break
                            worked_by_list_of_dicts.append(worked_by_dict_to_insert)
                            dict_to_insert.update({'worked_by': worked_by_list_of_dicts})
                            if len(row_to_insert[idx+count]) == 2:
                                indexes_to_skip.append(idx+count)
                                count += 1
                                while row_to_insert[idx+count][0] != 'Total Cost':
                                    parts_list_of_dicts.append({'parts_used': row_to_insert[idx+count][0], 'parts_cost': row_to_insert[idx+count][1]})
                                    indexes_to_skip.append(idx+count)
                                    count += 1
                                dict_to_insert.update({'parts': parts_list_of_dicts})
                            elif len(row_to_insert[idx+count]) == 4:
                                indexes_to_skip.append(idx+count)
                                dict_to_insert.update({'parts': [{row_to_insert[idx+count][0]: row_to_insert[idx+count][1], row_to_insert[idx+count][2]: row_to_insert[idx+count][3]}]})
                        else:
                            if row[0] == 'parts_used' and len(row) == 2:
                                count = 1
                                while row_to_insert[idx+count][0] != 'Total Cost':
                                    parts_list_of_dicts.append({'parts_used': row_to_insert[idx+count][0], 'parts_cost': row_to_insert[idx+count][1]})
                                    indexes_to_skip.append(idx+count)
                                    count += 1
                                dict_to_insert.update({'parts': parts_list_of_dicts})
                                continue
                            elif row[0] == 'parts_used' and len(row) == 4:
                                dict_to_insert.update({'parts': [{row[0]: row[1], row[2]: row[3]}]})
                                continue
                            for i in range(0, len(row), 2):
                                if i + 1 < len(row):
                                    dict_to_insert.update({row[i]: row[i+1]})
                                else:
                                    continue
                    elif row == ['New Row Started']:
                        dict_to_insert.update({'Property': propertyname, 'Onsiteid': onsiteid})
                        list_of_dicts.append(dict_to_insert)
                        dict_to_insert = {}
                        worked_by_dict_to_insert = {}
                        worked_by_list_of_dicts = []
                        parts_list_of_dicts = []
        self.df = pd.DataFrame(list_of_dicts, dtype = str)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class DelinquentReports(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        df = pd.read_excel(self.file, skiprows=2, nrows=1, dtype = str)
        df.dropna(axis=1, how='all', inplace=True)
        columns = df.columns
        fiscal_string = df[columns[0]][0]
        fiscal_period = fiscal_string.split(" ")[2]
        fiscal_as_of = fiscal_string.split(" ")[-1]
        df = pd.read_excel(self.file, skiprows=18, dtype = str)
        count = 0
        if not df.empty:
            count = 1
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
                                'Unit': 'unit_number'}
                    ,inplace=True)
            columns = df.columns
            if 'unit_number' not in columns:
                raise Exception("Column names are not set properly")
            for col in columns:
                if col.startswith('Unnamed'):
                    continue
                if df[col].isnull().all():
                    df[col] = 'none'
            mask = df['unit_number'].notna() & df['unit_number'].str.startswith('**')
            drop_index = df.index[mask][0]
            df = df.iloc[:drop_index]
            df.dropna(how='all', inplace=True)
            df = df.dropna(axis=1, how='all')
            df.reset_index(inplace=True, drop=True)   
            columns = df.columns
            column_to_drop = columns[4]
            mask = df[column_to_drop].notna() & df[column_to_drop].str.startswith('Grand Totals:')
            drop_index = df.index[mask][0]
            df = df.iloc[:drop_index]
            for col in columns:
                if col.startswith('Unnamed'):
                    continue
                if df[col].isnull().all():
                    df[col] = 'none'
            df.dropna(how='all', inplace=True)
            df = df.dropna(axis=1, how='all')
            df.reset_index(inplace=True, drop=True)        
            mask = df['unit_number'].notna() & df['unit_number'].str.startswith('Detail')
            drop_indices_detail = df.index[mask]
            mask = df['unit_number'].notna() & df['unit_number'].str.startswith('Bldg')
            drop_indices_bldg = df.index[mask]
            mask = df['unit_number'].notna() & df['unit_number'].str.startswith('DEL')
            drop_indices_del = df.index[mask]
            mask = df['unit_number'].notna() & df['unit_number'].str.startswith('PPD')
            drop_indices_ppd = df.index[mask]
            mask = df['unit_number'].notna() & df['unit_number'].str.startswith('Unit')
            drop_indices_unit = df.index[mask]
            columns = df.columns
            mask = df[columns[1]].notna() & df[columns[1]].str.startswith('Name\n')
            drop_indices_name = df.index[mask]
            drop_indices = list(drop_indices_detail) + list(drop_indices_bldg) + list(drop_indices_name) + list(drop_indices_del) + list(drop_indices_ppd) + list(drop_indices_unit)
            df.drop(drop_indices, inplace=True)     
            for idx, col in enumerate(columns):
                if col == 'total_prepaid':
                    df[col] = df[columns[idx-1]]
                elif col == 'total_delinquent':
                    df[col] = df[columns[idx-1]]
                elif col == 'prorate_credits':
                    df[col] = df[columns[idx+1]]
                elif col == 'deposits_held':
                    df[col] = df[columns[idx+1]]
            df.reset_index(inplace=True, drop=True)
            columns = df.columns
            for idx, row in df.iterrows():
                unit_number = row['unit_number']
                move_in_move_out_date = row['move_in_move_out']
                delistatus = row['status']
                deliinfo = row[columns[1]]
                if not pd.isna(unit_number) and pd.isna(move_in_move_out_date):
                    df.at[idx, 'move_in_move_out'] = 'none'
                if not pd.isna(unit_number) and pd.isna(delistatus):
                    df.at[idx, 'status'] = 'none'
                if not pd.isna(unit_number) and pd.isna(deliinfo):
                    df.at[idx, columns[1]] = 'none'    
            df['unit_number'] = df['unit_number'].ffill()
            df[columns[1]] = df[columns[1]].ffill()
            df['status'] = df['status'].ffill()
            df['move_in_move_out'] = df['move_in_move_out'].ffill()
            df['net_balance'] = df['net_balance'].bfill()
            df['deposits_held'] = df['deposits_held'].ffill()
            df['outstanding_deposit'] = df['outstanding_deposit'].ffill()
            df['late_nsf'] = df['late_nsf'].ffill() 
            mask = df['code_description'].isna()
            drop_indices = df.index[mask]
            df.drop(list(drop_indices), inplace=True)
            mask = df['code_description'].str.startswith('Subtotal')
            drop_indices = df.index[mask]
            df.drop(list(drop_indices), inplace=True)
            df.rename(columns={columns[1]: 'name_number_email'},inplace=True)
            df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
            df.reset_index(inplace=True, drop=True)
            som = df['name_number_email'].str.split('\n')
            for idx,i in enumerate(som):
                len_of_list = len(i)
                if len_of_list != 3:
                    has_email = False
                    for j in range(len_of_list):
                        if '@' in i[j]:
                            has_email = True
                    if has_email:
                        som[idx].insert(1, 'none')
                    else:
                        som[idx].append('none')
                    if len(som[idx]) == 2:
                        som[idx].append('none')
            names = []
            phone_numbers = []
            emails = []
            for i in som:
                names.append(i[0])
                phone_numbers.append(i[1])
                emails.append(i[2])
            df['name'] = names
            df['phone_number'] = phone_numbers
            df['email'] = emails 
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
            df['fiscal_period'] = fiscal_period
            df['fiscal_as_of'] = fiscal_as_of
            df['property_name'] = file_name.split("_")[1]
            df["onsiteid"] = file_name.split("_")[0]
            df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self.df = df
            return count 
        else:
            self.df = df 
            return count

class LeasereportData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        #strings_in_filepath = (os.path.basename(self.file)).split("_")
        #self.df["On Site id"] = strings_in_filepath[0]
        #self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class LeasetradereportData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        #strings_in_filepath = (os.path.basename(self.file)).split("_")
        #self.df["On Site id"] = strings_in_filepath[0]
        #self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class GoogleReviews(Data):
    def structurize_data(self):
        self.df = self.read_csv_file_without_header(dtype=str)
        self.df.columns = ['Property_Name', 'ReviewDate', 'Google_Stars', 'Google_Reviews']
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class MarketRateReportData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class GeneralLedgerReportData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        columns_to_drop=['Unnamed: 0','Unnamed: 15']
        self.df.drop(columns=columns_to_drop, inplace=True)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class UnitAmenitiesReport(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class UnitRentSummaryReport(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")
        

class UnitCapXReport(Data):
    def structurize_data(self):
        self.df = self.read_csv_file_tab_delimeted(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class UnitRehabReport(Data):
    def structurize_data(self):
        self.df = self.read_csv_file_tab_delimeted(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class UnitStandardTurnReport(Data):
    def structurize_data(self):
        self.df = self.read_csv_file_tab_delimeted(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class UnitRenewalOfferAnalysis(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class LeaseActivityData(Data):
    def structurize_data(self):
        self.df = self.ready_activity_report_xml()
        strings_in_filepath = (os.path.basename(self.file)).split("_")
        self.df["On Site id"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class UnitAvailbilityData(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="UnitAvailabilityReport")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class UnitSetupView(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)

        strings_in_filepath = (os.path.basename(self.file)).split("_")
        self.df["On Site id"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class ServiceRequestData(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class DelinquentReportsNew(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        now = datetime.now()
        xls = pd.ExcelFile(self.file)
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

class ResidentDetails(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        strings_in_filepath = (os.path.basename(self.file)).split("_")
        self.df["On Site id"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class SOCIReportDetails(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        parts = file_name.split('_')
        date_str = f"{parts[1]}_{parts[2]}_{parts[3].split('.')[0]}"
        self.df['SnapShotDate'] = date_str
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class SOCIReviewFeed(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class KnockActivityDaily(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df['StartDate'] = self.df.apply(lambda x: x['Unnamed: 29'] if x['Activity Report'] == 'startDate' else pd.NaT, axis=1)
        self.df['EndDate'] = self.df.apply(lambda x: x['Unnamed: 29'] if x['Activity Report'] == 'endDate' else pd.NaT, axis=1)
        self.df['StartDate'] = self.df['StartDate'].ffill().bfill()
        self.df['EndDate'] = self.df['EndDate'].ffill().bfill()
        self.df = self.df.drop(columns=['Activity Report', 'Unnamed: 29', 'Unnamed: 27'])
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class KnockCoversionReport(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        self.df = self.read_knock_conversion_csv()
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        date_str = file_name.split("_")[1].split(".")[0]
        date_value = datetime.strptime(date_str, "%m%d%Y").date()
        self.df['SnapShotDate'] = date_value
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class YELPReviewFeed(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIEllisLoyaltyDataWeekly(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        start_date_str, end_date_str =file_name.split('-')[0], file_name.split('-')[1].replace('.csv', '')
        week_start_date = datetime.strptime(start_date_str, "%m.%d.%Y").strftime("%m.%d.%Y")
        week_end_date = datetime.strptime(end_date_str, "%m.%d.%Y").strftime("%m.%d.%Y")
        self.df = self.read_csv_file_without_header(dtype=str)
        header_mapping = {
            'Unnamed: 0': '',
            'Unnamed: 2': 'Lead Conversion',
            'Unnamed: 4': 'Maintenance',
            'Unnamed: 6': 'Move-In',
            'Unnamed: 8': 'Move-Out',
            'Unnamed: 10': 'Pre Renewal'
        }
        self.df.iloc[0] = self.df.iloc[0].replace(header_mapping)
        new_header = self.df.iloc[0] + "-" + self.df.iloc[1].fillna('')
        new_header = new_header.str.strip("-").str.strip()  
        self.df.columns = new_header
        self.df = self.df[2:]
        self.df['weekstartdate'] = week_start_date
        self.df['weekenddate'] = week_end_date
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIEllisLoyaltyDataMonthly(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        month_year_str = file_name.replace('.csv', '').replace('.', '')
        self.df = self.read_csv_file_without_header(dtype=str)
        header_mapping = {
            'Unnamed: 0': '',
            'Unnamed: 2': 'Lead Conversion',
            'Unnamed: 4': 'Maintenance',
            'Unnamed: 6': 'Move-In',
            'Unnamed: 8': 'Move-Out',
            'Unnamed: 10': 'Pre Renewal'
        }
        self.df.iloc[0] = self.df.iloc[0].replace(header_mapping)
        new_header = self.df.iloc[0] + "-" + self.df.iloc[1].fillna('')
        new_header = new_header.str.strip("-").str.strip()  
        self.df.columns = new_header
        self.df = self.df[2:]
        self.df['monthyear'] = month_year_str
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIEllisKPIDataWeekly(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        start_date_str, end_date_str =file_name.split('-')[0], file_name.split('-')[1].replace('.csv', '')
        week_start_date = datetime.strptime(start_date_str, "%m.%d.%Y").strftime("%m.%d.%Y")
        week_end_date = datetime.strptime(end_date_str, "%m.%d.%Y").strftime("%m.%d.%Y")
        self.df = self.read_csv_file_without_header(dtype=str)
        self.df = self.df.iloc[1:]
        self.df.columns = self.df .iloc[0]
        self.df = self.df[1:]
        self.df = self.df.rename(columns={"    Property Name": "Property Name"})
        self.df["Custom"] = self.df["Property Name"].apply(self.add_custom_compaign_column)
        self.df["Custom"] = self.df["Custom"].fillna(method="ffill")
        self.df = self.df.rename(columns={"Column10": "Service Category"})
        excluded_names = ["Lead Conversion", "Maintenance", "Move-In", "Move-Out", "Pre Renewal", "Total"]
        self.df = self.df[~self.df["Property Name"].str.strip().isin(excluded_names)]
        self.df['weekstartdate'] = week_start_date
        self.df['weekenddate'] = week_end_date
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIEllisKPIDataMonthly(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        month_year_str = file_name.replace('.csv', '').replace('.', '')
        self.df = self.read_csv_file_without_header(dtype=str)
        self.df = self.df.iloc[1:]
        self.df.columns = self.df .iloc[0]
        self.df = self.df[1:]
        self.df = self.df.rename(columns={"    Property Name": "Property Name"})
        self.df["Custom"] = self.df["Property Name"].apply(self.add_custom_compaign_column)
        self.df["Custom"] = self.df["Custom"].fillna(method="ffill")
        self.df = self.df.rename(columns={"Column10": "Service Category"})
        excluded_names = ["Lead Conversion", "Maintenance", "Move-In", "Move-Out", "Pre Renewal", "Total"]
        self.df = self.df[~self.df["Property Name"].str.strip().isin(excluded_names)]
        self.df['monthyear'] = month_year_str
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class EllisSurveyReport(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="Data1")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class KnockEngagementReportDaily(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df['StartDate'] = self.df.apply(lambda x: x['Unnamed: 8'] if x['Engagement Report'] == 'startDate' else pd.NaT, axis=1)
        self.df['EndDate'] = self.df.apply(lambda x: x['Unnamed: 8'] if x['Engagement Report'] == 'endDate' else pd.NaT, axis=1)
        self.df['StartDate'] = self.df['StartDate'].ffill().bfill()
        self.df['EndDate'] = self.df['EndDate'].ffill().bfill()
        self.df = self.df.drop(columns=['Engagement Report','Unnamed: 6', 'Unnamed: 8'])
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class RentGrataWeeklyReport(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        self.df = self.read_csv_file()
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        date_str = file_name.split("_")[1].split(".")[0]
        date_value = datetime.strptime(date_str, "%Y-%m-%d").date()
        self.df['SnapShotDate'] = date_value
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class RentGrataUnpaidRewards(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        self.df = self.read_csv_file()
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIBannersErrors(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="Current Year")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        
        static_cols = [
                   "Property Code", "Project ID", "Property", "Sub Group", "GL Code", "Lender Item", "Capital Type", "Carryover", "Project Name", "Current Year Description", "Current Capital Start Date", "Current Capital End Date", "Status", "Capital Notes", "New Note", "Budget", "PY Accrual", "Projected Total Spend", "Approved Spend", "Unapproved Spend", "Budget Variance", "Paid to Date", "Prior Year Paid", "Prior Paid + Current", "Check", "Original Capital Start Date", "Original Capital End Date", "Project Plan", "T60 Notes", "Critical Project", "Show Project in Long Term", "Project Owner", "Invoice auto-assign"
                     ]
        self.df = self.df.melt(id_vars=static_cols, 
                     var_name="Month", 
                     value_name="Spend")
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIBannersPendingInvoice(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="Pending Invoice List")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIBannersAssignedInvoice(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="Assigned Invoice List")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")        

class BIBannersDVInvoice(Data):
    def structurize_data(self):
        self.df = self.read_excel_file(sheet_name="DV Invoices")
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class BIRentGrataUnpaidRewards(Data):
    def structurize_data(self):
        base_columns = [
                        'Listing name', 'Recipient name', 'Recipient email', 'Recipient phone',
                        'Recipient type', 'Recipient unit number', 'Prospect name',
                        'Prospect email', 'Prospect moved in date', 'Past due days',
                        'Conversation start date', 'Reward amount', 'Manager link'
                       ]
        extended_columns = base_columns.copy()
        extended_columns.insert(5, 'property address')  
        rows = []
        with open(self.file, encoding='utf-8') as f:
            header = f.readline().strip().split(',')
            for line in f:
                values = line.strip().split(',')
                if len(values) == len(base_columns):  
                    row = dict(zip(base_columns, values))
                    row['property address'] = '' 
                elif len(values) == len(extended_columns):  
                    row = dict(zip(extended_columns, values))
                else:
                    if len(values) == len(base_columns) + 1:
                        temp_values = values[:5] + [values[5]] + values[6:]
                        row = dict(zip(extended_columns, temp_values))
                    else:
                        continue
                rows.append(row)
        self.df  = pd.DataFrame(rows)
        self.df  = self.df [extended_columns]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")
        snahpshot_date = self.file.split('__')[-1].split('.')[0]
        self.df['snahpshot_date'] = snahpshot_date


class LeaseActivityVisits(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        strings_in_filepath = (os.path.basename(self.file)).split("_")
        self.df["OneSiteid"] = strings_in_filepath[0]
        self.df["Property Name"] = strings_in_filepath[1]
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")


class RentGrataDailyReport(Data):
    def structurize_data(self):
        file_name = os.path.basename(self.file)
        self.df = self.read_csv_file()
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df = self.df.apply(lambda x: x.str.replace(")", "").str.replace("(", "-"))
        self.df.replace(",", "", regex=True, inplace=True)
        date_str = file_name.split("__")[1].split(".")[0]
        date_value = datetime.strptime(date_str, "%m-%d-%Y").date()
        self.df['SnapShotDate'] = date_value
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")

class UnitAvailbilityDataNew(Data):
    def structurize_data(self):
        self.df = self.read_csv_file(dtype=str)
        self.df = self.replace_nan_values(self.df)
        self.df = self.convert_to_string(self.df)
        self.df["Insert_date"] = now.strftime("%Y-%m-%d %H:%M:%S")



'''
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
