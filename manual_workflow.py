from database import MSSQL
from transform_data import (
    LeaseExpirationData,
    LeaseDetailData,
    AllUnitsData,
    ResidentDemographicsData,
    EntityData,
    AcquisitionData,
    PropertyInfoData,
    AvailabiltyData,
    TurnoverData,
    IncomeBudgetData,
    WeeklyReports,
    RenewalsFridayReports,
    MaintainenceReports,
    DelinquentReports,
    LeasereportData,
    LeasetradereportData,
    GoogleReviews,
    MarketRateReportData,
    GeneralLedgerReportData,
    UnitAmenitiesReport,
    UnitRentSummaryReport,
    UnitCapXReport,
    UnitRehabReport,
    UnitStandardTurnReport,
    UnitRenewalOfferAnalysis,
    LeaseActivityData,
    UnitAvailbilityData,
    UnitSetupView,
    ServiceRequestData,
    DelinquentReportsNew,
    ResidentDetails,
    SOCIReportDetails,
    SOCIReviewFeed,
    KnockActivityDaily,
    KnockCoversionReport,
    YELPReviewFeed,
    BIEllisLoyaltyDataWeekly,
    BIEllisLoyaltyDataMonthly,
    BIEllisKPIDataWeekly,
    BIEllisKPIDataMonthly,
    EllisSurveyReport,
    KnockEngagementReportDaily,
    RentGrataWeeklyReport,
    RentGrataUnpaidRewards,
    BIBannersErrors,
    BIBannersPendingInvoice,
    BIBannersAssignedInvoice,
    BIBannersDVInvoice,
    BIRentGrataUnpaidRewards,
    LeaseActivityVisits,
    RentGrataDailyReport
)
import os
import glob
from dotenv import load_dotenv
import logging
from database import MSSQL
from database import PostgreSQL
import time
from prefect_ps_sql_load.load_contact_level_details import ContactLevelDetailsLoader
from prefect_ps_sql_load.variable import *

load_dotenv()
logging.basicConfig(
    filename="manual_workflow.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)


def load_weekly_files():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = [os.getenv("WEEKLY_TABLE_NAME")]
    ms_sql.truncate_tables(table_list=table_list)

    weekly_files = os.getenv("WEEKLY_FILE_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("WEEKLY_TABLE_NAME")} table'
    )
    start_time = time.time()
    for file in os.listdir(weekly_files):
        file_path = os.path.join(weekly_files, file)
        weekly_data = WeeklyReports(file_path)
        count = weekly_data.structurize_data()
        if count == 1:
            ms_sql.insert_dataframe(os.getenv("WEEKLY_TABLE_NAME"), weekly_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("WEEKLY_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info("The script was successfull")


def income_statement(ms_sql):
    logging.info("started working on income statement")
    income_data = IncomeBudgetData(os.getenv("INCOME_DATA_FILEPATH_FIX"))
    income_data.structurize_data()
    ms_sql.insert_dataframe(
        table_name=os.getenv("INCOME_STAGGING_TABLE_NAME"), df=income_data.df
    )


def budget_statement(ms_sql):
    logging.info("started working on budeget statement")
    budget_data = IncomeBudgetData(os.getenv("BUDGET_DATA_FILEPATH"))
    budget_data.structurize_data()
    ms_sql.insert_dataframe(
        table_name=os.getenv("BUDGET_STAGGING_TABLE_NAME"), df=budget_data.df
    )


def process_finance_statements_to_sql():
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


def load_availability_turnover():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    availability_files = glob.glob(
        os.path.join(os.getenv("MULTIPLE_PROPERTY_DATA"), "*Availability*.xls")
    )
    turnover_files = glob.glob(
        os.path.join(os.getenv("MULTIPLE_PROPERTY_DATA"), "*turnover*.xls")
    )

    table_list = [
        os.getenv("AVAILABILITY_TABLE_NAME"),
        os.getenv("TURNOVER_TABLE_NAME"),
    ]
    ms_sql.truncate_tables(table_list=table_list)

    logging.info(
        f'Starting to stage data in the {os.getenv("AVAILABILITY_TABLE_NAME")} table'
    )
    '''
    start_time = time.time()
    for availabilty_file in availability_files:
        print(availabilty_file)
        availability_data = AvailabiltyData(availabilty_file)
        availability_data.structurize_data()
        if not availability_data.df is None:
            ms_sql.insert_dataframe(
                os.getenv("AVAILABILITY_TABLE_NAME"), availability_data.df
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("AVAILABILITY_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info(
        f'Starting to stage data in the {os.getenv("TURNOVER_TABLE_NAME")} table'
    )'''
    start_time = time.time()
    for turnover_file in turnover_files:
        turnover_data = TurnoverData(turnover_file)
        turnover_data.structurize_data()
        ms_sql.insert_dataframe(os.getenv("TURNOVER_TABLE_NAME"), turnover_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("TURNOVER_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info("load_availability_turnover script was successfull")


def load_all_units_lease_deatails_lease_expiration():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("ALL_UNITS_TABLE_NAME"),
            os.getenv("LEASE_DETAILS_TABLE_NAME"),
            os.getenv("LEASE_EXPIRATION_TABLE_NAME"),
            os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME"),
            os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
            os.getenv("RESIDENT_DETAILS_REPORT_TABLE_NAME")
        }
    )
    directory = os.getenv("MULTIPLE_PROPERTY_DATA")

    start = time.time()
    logging.info(
        f'Starting to stage data in the {os.getenv("ALL_UNITS_TABLE_NAME")}, {os.getenv("LEASE_DETAILS_TABLE_NAME")}, {os.getenv("LEASE_EXPIRATION_TABLE_NAME")} , {os.getenv("LEASE_ACTIVITY_TABLE_NAME")} and {os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME")} tables'
    )
    for file in os.listdir(directory):
        print(file)
        full_path = os.path.join(directory, file)
        '''
        if "all units".lower() in file.lower():
            all_units_data = AllUnitsData(full_path)
            all_units_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("ALL_UNITS_TABLE_NAME"), all_units_data.df
            )
      
        if "lease details".lower() in file.lower():
            lease_details_data = LeaseDetailData(full_path)
            lease_details_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("LEASE_DETAILS_TABLE_NAME"), lease_details_data.df
            )
        '''
        if "lease expiration".lower() in file.lower():
            lease_expiration_data = LeaseExpirationData(full_path)
            lease_expiration_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("LEASE_EXPIRATION_TABLE_NAME"), lease_expiration_data.df
            )
        '''
        if "Resident Demographics".lower() in file.lower():
            resident_demographics_data = ResidentDemographicsData(full_path)
            resident_demographics_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME"),
                resident_demographics_data.df,
            )

        if "Leasing Activity Detail".lower() in file.lower():
            lease_activity_data = LeaseActivityData(full_path)
            lease_activity_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
                lease_activity_data.df,
            )
        '''
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("ALL_UNITS_TABLE_NAME")}, {os.getenv("LEASE_DETAILS_TABLE_NAME")}, {os.getenv("LEASE_EXPIRATION_TABLE_NAME")}, {os.getenv("LEASE_ACTIVITY_TABLE_NAME")} and {os.getenv("RESIDENT_DEMOGRAPHICS_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_acquisition_entities_propertyinfo():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("PROPERTY_INFO_TABLE_NAME"),
            os.getenv("ENTITIES_TABLE_NAME"),
            os.getenv("AQUISITION_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("AQUISITION_TABLE_NAME")} table'
    )
    start_time = time.time()
    acquisition_data = AcquisitionData(os.getenv("AQUISITION_FILEPATH"))
    acquisition_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("AQUISITION_TABLE_NAME"), acquisition_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("AQUISITION_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info(
        f'Starting to stage data in the {os.getenv("ENTITIES_TABLE_NAME")} table'
    )
    start_time = time.time()
    entities_data = EntityData(os.getenv("ENTITIES_FILEPATH"))
    entities_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("ENTITIES_TABLE_NAME"), entities_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("ENTITIES_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info(
        f'Starting to stage data in the {os.getenv("PROPERTY_INFO_TABLE_NAME")} table'
    )
    start_time = time.time()
    property_info_data = PropertyInfoData(os.getenv("PROPERTY_INFO_FILEPATH"))
    property_info_data.structurize_data()
    ms_sql.insert_dataframe(
        os.getenv("PROPERTY_INFO_TABLE_NAME"), property_info_data.df
    )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("PROPERTY_INFO_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

    logging.info("load_acquisition_entities_propertyinfo script was successfull")


def load_renewals_friday_reports():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables({os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME")})
    logging.info(
        f'Starting to stage data in the {os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME")} table'
    )
    friday_report_data = RenewalsFridayReports(
        os.getenv("FRIDAY_RENEWALS_REPORT_FILE_PATH")
    )
    friday_report_data.structurize_data()
    ms_sql.insert_dataframe(
        table_name=os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME"),
        df=friday_report_data.df,
    )
    logging.info(
        f'Sucessfully staged all data to {os.getenv("FRIDAY_RENEWALS_REPORT_TABLE_NAME")}'
    )
def load_delinquent_files():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = [os.getenv("DELINQUENT_TABLE_NAME")]
    ms_sql.truncate_tables(table_list=table_list)

    delinquent_folder = os.getenv("DELINQUENT_FOLDER_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("DELINQUENT_TABLE_NAME")} table'
    )
    start_time = time.time()
    for file in os.listdir(delinquent_folder):
        file_path = os.path.join(delinquent_folder, file)
        print(file_path)
        delinquent_data = DelinquentReportsNew(file_path)
        delinquent_df,count = delinquent_data.structurize_data()
        #print(delinquent_data.df)
        print(count)
        if count == 1:
            ms_sql.insert_dataframe(os.getenv("DELINQUENT_TABLE_NAME"), delinquent_df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("DELINQUENT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
    logging.info("The script was successfull")

def load_requests():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    table_list = [os.getenv("REQUESTS_TABLE_NAME")]
    ms_sql.truncate_tables(table_list=table_list)

    maintainence_folder = os.getenv("REQUESTS_FOLDER_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("REQUESTS_TABLE_NAME")} table'
    )
    start_time = time.time()
    maintainece_data = MaintainenceReports(maintainence_folder)
    maintainece_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("REQUESTS_TABLE_NAME"), maintainece_data.df)
    elapsed_time = (time.time() - start_time) / 60 
    logging.info(
        f'Sucessfully staged all data to {os.getenv("REQUESTS_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} minutes'
    )

    logging.info("The script was successfull")

def load_lease_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("LEASE_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("LEASE_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    lease_report_data = LeasereportData(os.getenv("LEASE_REPORT_FOLDER_PATH"))
    lease_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("LEASE_REPORT_TABLE_NAME"), lease_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("LEASE_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_lease_trade_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("LEASE_TRADE_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("LEASE_TRADE_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    lease_trade_report_data = LeasetradereportData(os.getenv("LEASE_TRADE_REPORT_FOLDER_PATH"))
    lease_trade_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("LEASE_TRADE_REPORT_TABLE_NAME"), lease_trade_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("LEASE_TRADE_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_google_reviews_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("GOOGLE_REVIEW_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("GOOGLE_REVIEW_TABLE_NAME")} table'
    )
    start_time = time.time()
    google_review_data = GoogleReviews(os.getenv("GOOGLE_REVIEW_FOLDER_PATH"))
    google_review_data.structurize_data()
    print(google_review_data.df)
    ms_sql.insert_dataframe(os.getenv("GOOGLE_REVIEW_TABLE_NAME"), google_review_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("GOOGLE_REVIEW_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_market_rate_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("MARKET_RATE_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("MARKET_RATE_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    market_rate_report_data = MarketRateReportData(os.getenv("MARKET_RATE_REPORT_FOLDER_PATH"))
    market_rate_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("MARKET_RATE_REPORT_TABLE_NAME"), market_rate_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("MARKET_RATE_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_general_ledger_turn_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("GENEREL_LEDGER_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("GENEREL_LEDGER_TABLE_NAME")} table'
    )
    start_time = time.time()
    general_ledger_report_data = GeneralLedgerReportData(os.getenv("GENEREL_LEDGER_REPORT_FOLDER_PATH"))
    general_ledger_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("GENEREL_LEDGER_TABLE_NAME"), general_ledger_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("GENEREL_LEDGER_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_unit_rent_summary_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_RENT_SUMMARY_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_RENT_SUMMARY_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_rent_summary_report_data = UnitRentSummaryReport(os.getenv("UNIT_RENT_SUMMARY_REPORT_FOLDER_PATH"))
    unit_rent_summary_report_data.structurize_data()
    unit_rent_summary_report_data_df=unit_rent_summary_report_data.df
    unit_rent_summary_report_data_df = unit_rent_summary_report_data_df.rename(columns={'Floor ': 'Floor Plan', 'Unit ': 'Unit Type' ,'Avg. Sq. ': 'Avg. Sq. Ft', 'Monthly Effective ': 'Monthly Effective Rent'})
    ms_sql.insert_dataframe(os.getenv("UNIT_RENT_SUMMARY_REPORT_TABLE_NAME"), unit_rent_summary_report_data_df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_RENT_SUMMARY_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_unit_amenities_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_amenties_report_data = UnitAmenitiesReport(os.getenv("UNIT_AMENITIES_REPORT_FOLDER_PATH"))
    unit_amenties_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME"), unit_amenties_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_AMENITIES_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_unit_turn_capx_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_turn_capx_report_data = UnitCapXReport(os.getenv("UNIT_TURN_CAPX_REPORT_FOLDER_PATH"))
    unit_turn_capx_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME"), unit_turn_capx_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_TURN_CAPX_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_unit_rehab_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_REHAB_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_REHAB_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_rehab_report_data = UnitRehabReport(os.getenv("UNIT_REHAB_REPORT_FOLDER_PATH"))
    unit_rehab_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_REHAB_REPORT_TABLE_NAME"), unit_rehab_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_REHAB_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    ) 


def load_unit_standard_turn_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_standard_turn_report_data = UnitStandardTurnReport(os.getenv("UNIT_STANDARD_TURN_REPORT_FOLDER_PATH"))
    unit_standard_turn_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME"), unit_standard_turn_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_STANDARD_TURN_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_unit_renewal_offer_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_renewal_offer_data = UnitRenewalOfferAnalysis(os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_FOLDER_PATH"))
    unit_renewal_offer_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME"), unit_renewal_offer_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_RENEWAL_OFFER_ANALYSIS_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_lease_activity_detail_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
        }
    )
    directory = os.getenv("MULTIPLE_PROPERTY_DATA")

    start = time.time()
    logging.info(
        f'Starting to stage data in  {os.getenv("LEASE_ACTIVITY_TABLE_NAME")} tables'
    )
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "Leasing Activity Detail".lower() in file.lower():
            lease_activity_data = LeaseActivityData(full_path)
            lease_activity_data.structurize_data()
            print(lease_activity_data.df)
            ms_sql.insert_dataframe(
                os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
                lease_activity_data.df,
            )
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("LEASE_ACTIVITY_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_unit_availbility_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_AVAILBILITY_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("UNIT_AVAILBILITY_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    unit_availbility_report_data = UnitAvailbilityData(os.getenv("UNIT_AVAILBILITY_REPORT_FOLDER_PATH"))
    unit_availbility_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("UNIT_AVAILBILITY_REPORT_TABLE_NAME"), unit_availbility_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_AVAILBILITY_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

def unit_setup_view():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("UNIT_SETUP_VIEW_TABLE_NAME"),
        }
    )
    directory = os.getenv("MULTIPLE_PROPERTY_DATA")

    start = time.time()
    logging.info(
        f'Starting to stage data in  {os.getenv("UNIT_SETUP_VIEW_TABLE_NAME")} tables'
    )
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        print(full_path)
        
        if "Unit Setup View".lower() in file.lower():
            unit_setup_view = UnitSetupView(full_path)
            unit_setup_view.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("UNIT_SETUP_VIEW_TABLE_NAME"),
                unit_setup_view.df,
            )
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("UNIT_SETUP_VIEW_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_service_requests_new():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    service_request_report_data = ServiceRequestData(os.getenv("SERVICE_REQUESTS_REPORT_FOLDER_PATH"))
    service_request_report_data.structurize_data()
    ms_sql.insert_dataframe(os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME"), service_request_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SERVICE_REQUESTS_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_resident_details_new():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("RESIDENT_DETAILS_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("RESIDENT_DETAILS_REPORT_TABLE_NAME")} table'
    )
    start_time = time.time()
    directory = os.getenv("MULTIPLE_PROPERTY_DATA")
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "contact".lower() in file.lower():
            print(full_path)
            resident_details_report_data = ResidentDetails(full_path)
            resident_details_report_data.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("RESIDENT_DETAILS_REPORT_TABLE_NAME"),
                resident_details_report_data.df,
            )
   
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("RESIDENT_DETAILS_REPORT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_soci_report_details():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("SOCI_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("SOCI_REPORT_TABLE_NAME")} table'
    )
    directory = os.getenv("SOCI_REPORT_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "soci".lower() in file.lower():
            soci_report_data = SOCIReportDetails(full_path)
            soci_report_data.structurize_data()
            print(soci_report_data.df)
            ms_sql.insert_dataframe(
                os.getenv("SOCI_REPORT_TABLE_NAME"),
                soci_report_data.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SOCI_REPORT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_soci_review_details():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("SOCI_REVIEW_FEED_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("SOCI_REVIEW_FEED_TABLE_NAME")} table'
    )
    start_time = time.time()
    soci_review_feed_report_data = SOCIReviewFeed(os.getenv("SOCI_REVIEW_FEED_FOLDER_PATH"))
    soci_review_feed_report_data.structurize_data()

    ms_sql.insert_dataframe(os.getenv("SOCI_REVIEW_FEED_TABLE_NAME"), soci_review_feed_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("SOCI_REVIEW_FEED_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )



def load_knock_activity_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("KNOCK_ACTIVITY_TABLE_NAME"),
            os.getenv("KNOCK_ACTIVITY_WEEKLY_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("KNOCK_ACTIVITY_TABLE_NAME")} , {os.getenv("KNOCK_ACTIVITY_WEEKLY_TABLE_NAME")} table'
    )
    directory = os.getenv("KNOCK_ACTIVITY_FOLDER_PATH")
    start_time = time.time()
    '''
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "activity".lower() in file.lower():
            knock_activity_report = KnockActivityDaily(full_path)
            knock_activity_report.structurize_data()
            print(knock_activity_report.df)
            ms_sql.insert_dataframe(
                os.getenv("KNOCK_ACTIVITY_TABLE_NAME"),
                knock_activity_report.df,
            )
    '''
    weekly_directory = os.getenv("KNOCK_ACTIVITY_WEEKLY_FOLDER_PATH")

    for file in os.listdir(weekly_directory):
        full_path = os.path.join(weekly_directory, file)
        
        if "activity".lower() in file.lower():
            knock_activity_report = KnockActivityDaily(full_path)
            knock_activity_report.structurize_data()
            print(knock_activity_report.df)
            ms_sql.insert_dataframe(
                os.getenv("KNOCK_ACTIVITY_WEEKLY_TABLE_NAME"),
                knock_activity_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("KNOCK_ACTIVITY_TABLE_NAME")}, {os.getenv("KNOCK_ACTIVITY_WEEKLY_TABLE_NAME")}tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_knock_conversion_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("KNOCK_CONVERSION_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("KNOCK_CONVERSION_TABLE_NAME")} table'
    )
    directory = os.getenv("KNOCK_CONVERSION_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        print(full_path)
        
        if "conversion".lower() in file.lower():
            knock_conversion_report = KnockCoversionReport(full_path)
            knock_conversion_report.structurize_data()
            print(knock_conversion_report.df)
            ms_sql.insert_dataframe(
                os.getenv("KNOCK_CONVERSION_TABLE_NAME"),
                knock_conversion_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("KNOCK_CONVERSION_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_yelp_review_details():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("YELP_REVIEWS_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("YELP_REVIEWS_TABLE_NAME")} table'
    )
    start_time = time.time()
    soci_review_feed_report_data = SOCIReviewFeed(os.getenv("YELP_REVIEWS_FOLDER_PATH"))
    soci_review_feed_report_data.structurize_data()

    ms_sql.insert_dataframe(os.getenv("YELP_REVIEWS_TABLE_NAME"), soci_review_feed_report_data.df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("YELP_REVIEWS_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_ellis_bi_report_weekly():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_ELLIS_LOYALITY_WEEKLY_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_ELLIS_LOYALITY_WEEKLY_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_ELLIS_LOYALITY_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "-".lower() in file.lower():
            bi_ellis_loyality_report = BIEllisLoyaltyDataWeekly(full_path)
            bi_ellis_loyality_report.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("BI_ELLIS_LOYALITY_WEEKLY_TABLE_NAME"),
                bi_ellis_loyality_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_ELLIS_LOYALITY_WEEKLY_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_ellis_bi_report_monthly():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_ELLIS_LOYALITY_MONTHLY_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_ELLIS_LOYALITY_MONTHLY_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_ELLIS_LOYALITY_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "-" not in file.lower():
            bi_ellis_loyality_report = BIEllisLoyaltyDataMonthly(full_path)
            bi_ellis_loyality_report.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("BI_ELLIS_LOYALITY_MONTHLY_TABLE_NAME"),
                bi_ellis_loyality_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_ELLIS_LOYALITY_MONTHLY_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_ellis_KPI_report_weekly():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_ELLIS_KPI_WEEKLY_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_ELLIS_LOYALITY_WEEKLY_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_ELLIS_KPI_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "-".lower() in file.lower():
            bi_ellis_kpi_report = BIEllisKPIDataWeekly(full_path)
            bi_ellis_kpi_report.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("BI_ELLIS_KPI_WEEKLY_TABLE_NAME"),
                bi_ellis_kpi_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_ELLIS_KPI_WEEKLY_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_ellis_KPI_report_monthly():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_ELLIS_KPI_MONTHLY_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_ELLIS_KPI_MONTHLY_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_ELLIS_KPI_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "-" not in file.lower():
            bi_ellis_kpi_report = BIEllisKPIDataMonthly(full_path)
            bi_ellis_kpi_report.structurize_data()
            ms_sql.insert_dataframe(
                os.getenv("BI_ELLIS_KPI_MONTHLY_TABLE_NAME"),
                bi_ellis_kpi_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_ELLIS_KPI_MONTHLY_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_ellis_survey_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_ELLIS_SURVEY_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_ELLIS_SURVEY_REPORT_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_ELLIS_SURVEY_REPORT_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        bi_ellis_survey_report = EllisSurveyReport(full_path)
        bi_ellis_survey_report.structurize_data()
        ms_sql.insert_dataframe(
            os.getenv("BI_ELLIS_SURVEY_REPORT_TABLE_NAME"),
            bi_ellis_survey_report.df,
        )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_ELLIS_SURVEY_REPORT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_knock_engagement_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME")} table'
    )
    directory = os.getenv("KNOCK_ENGAGEMENT_FOLDER_PATH")
    start_time = time.time()
    knock_engagement_report = KnockEngagementReportDaily(os.getenv("KNOCK_ENGAGEMENT_FOLDER_PATH"))
    knock_engagement_report.structurize_data()
    print(knock_engagement_report.df)
    ms_sql.insert_dataframe(
        os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME"),
        knock_engagement_report.df,
    )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("KNOCK_ENGAGEMENT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_rent_grata_weekly_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        bi_rent_grata_weekly_report = RentGrataWeeklyReport(full_path)
        bi_rent_grata_weekly_report.structurize_data()
        ms_sql.insert_dataframe(
            os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME"),
            bi_rent_grata_weekly_report.df,
        )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_rent_grata_unpaid_rewards_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_RENT_GRATA_Unpaid_Rewards_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_RENT_GRATA_Unpaid_Rewards_TABLE_NAME")} table'
    )
    directory = os.getenv("BI_RENT_GRATA_Unpaid_Rewards_FOLDER_PATH")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        bi_rent_grata_unpaid_reward_report = BIRentGrataUnpaidRewards(full_path)
        bi_rent_grata_unpaid_reward_report.structurize_data()
        ms_sql.insert_dataframe(
            os.getenv("BI_RENT_GRATA_Unpaid_Rewards_TABLE_NAME"),
            bi_rent_grata_unpaid_reward_report.df,
        )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_RENT_GRATA_Unpaid_Rewards_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_bi_banners_error_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_BANNER_ERROR_TABLE_NAME"),
            os.getenv("BI_BANNER_ERROR_ASSIGNED_INVOICES_TABLE_NAME"),
            os.getenv("BI_BANNER_ERROR_PENDING_INVOICES_TABLE_NAME"),
            os.getenv("BI_BANNER_ERROR_DV_INVOICES_TABLE_NAME"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_BANNER_ERROR_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_PENDING_INVOICES_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_ASSIGNED_INVOICES_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_DV_INVOICES_TABLE_NAME")} tables'
    )
    directory = os.getenv("BI_BANNER_ERROR_FOLDER_PATH")
    start_time = time.time()
    bi_banner_error = BIBannersErrors(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_error.structurize_data()
    ms_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_TABLE_NAME"),
        bi_banner_error.df,
    )

    bi_banner_pending_invoice = BIBannersPendingInvoice(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_pending_invoice.structurize_data()
    ms_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_PENDING_INVOICES_TABLE_NAME"),
        bi_banner_pending_invoice.df,
    )

    bi_banner_assigned_invoice = BIBannersAssignedInvoice(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_assigned_invoice.structurize_data()
    ms_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_ASSIGNED_INVOICES_TABLE_NAME"),
        bi_banner_assigned_invoice.df,
    )

    bi_banner_dv_invoice = BIBannersDVInvoice(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_dv_invoice.structurize_data()
    ms_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_DV_INVOICES_TABLE_NAME"),
        bi_banner_dv_invoice.df,
    )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_BANNER_ERROR_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_PENDING_INVOICES_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_ASSIGNED_INVOICES_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_DV_INVOICES_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_availability_onetime():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()

    availability_files = glob.glob(
        os.path.join(os.getenv("MULTIPLE_PROPERTY_DATA_ONETIME"), "*Availability*.xls")
    )
    
    table_list = [
        os.getenv("AVAILABILITY_TABLE_NAME"),
    ]
    ms_sql.truncate_tables(table_list=table_list)

    logging.info(
        f'Starting to stage data in the {os.getenv("AVAILABILITY_TABLE_NAME")} table'
    )
    start_time = time.time()
    for availabilty_file in availability_files:
        print(availabilty_file)
        availability_data = AvailabiltyData(availabilty_file)
        availability_data.structurize_data()
        if not availability_data.df is None:
            ms_sql.insert_dataframe(
                os.getenv("AVAILABILITY_TABLE_NAME"), availability_data.df
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("AVAILABILITY_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )

def load_lease_activity_new():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("LEASE_ACTIVITY_NEW_TABLE_NAME"),
        }
    )
    directory = os.getenv("LEASE_ACTIVITY_NEW_FOLDER_PATH")

    start = time.time()
    logging.info(
        f'Starting to stage data in  {os.getenv("LEASE_ACTIVITY_NEW_TABLE_NAME")} tables'
    )

    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "activity".lower() in file.lower():
            try:
                print(full_path)
                lease_activity_visits = LeaseActivityVisits(full_path)
                lease_activity_visits.structurize_data()
                ms_sql.insert_dataframe(
                    os.getenv("LEASE_ACTIVITY_NEW_TABLE_NAME"),
                    lease_activity_visits.df,
                )
            except Exception as e:
                print(full_path)
                logging.error("Script failed to run ", exc_info=e)
            
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )   


def load_rent_grata_daily_report():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.truncate_tables(
        {
            os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME_DAILY"),
        }
    )
    logging.info(
        f'Starting to stage data in the {os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME_DAILY")} table'
    )
    directory = os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_FOLDER_PATH_DAILY")
    start_time = time.time()
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "olympus".lower() in file.lower():
            rent_grata_daily_report = RentGrataDailyReport(full_path)
            rent_grata_daily_report.structurize_data()
            print(rent_grata_daily_report.df)
            ms_sql.insert_dataframe(
                os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME_DAILY"),
                rent_grata_daily_report.df,
            )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_RENT_GRATA_WEEKLY_REPORT_TABLE_NAME_DAILY")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )


def load_delinquent_files_pssql():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()

    table_list = [os.getenv("DELINQUENT_TABLE_NAME")]
    #ps_sql.truncate_tables(table_list=table_list)

    delinquent_folder = os.getenv("DELINQUENT_FOLDER_PATH")
    logging.info(
        f'Starting to stage data in the {os.getenv("DELINQUENT_TABLE_NAME")} table'
    )
    start_time = time.time()
    for file in os.listdir(delinquent_folder):
        file_path = os.path.join(delinquent_folder, file)
        print(file_path)
        delinquent_data = DelinquentReportsNew(file_path)
        delinquent_df,count = delinquent_data.structurize_data()
        #print(delinquent_data.df)
        print(count)
        if count == 1:
            ps_sql.insert_dataframe(os.getenv("DELINQUENT_TABLE_NAME"), delinquent_df)
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("DELINQUENT_TABLE_NAME")}: Elapsed time {elapsed_time:.2f} seconds'
    )
    logging.info("The script was successfull")


def load_lease_activity_detail_report_psql():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
        {
            os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
        }
    )
    directory = os.getenv("MULTIPLE_PROPERTY_DATA")

    start = time.time()
    logging.info(
        f'Starting to stage data in  {os.getenv("LEASE_ACTIVITY_TABLE_NAME")} tables'
    )
    for file in os.listdir(directory):
        full_path = os.path.join(directory, file)
        
        if "Leasing Activity Detail".lower() in file.lower():
            lease_activity_data = LeaseActivityData(full_path)
            lease_activity_data.structurize_data()
            print(lease_activity_data.df)
            ps_sql.insert_dataframe(
                os.getenv("LEASE_ACTIVITY_TABLE_NAME"),
                lease_activity_data.df,
            )
    elapsed_time = time.time() - start
    logging.info(
        f'Sucessfully staged all data to {os.getenv("LEASE_ACTIVITY_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )

def execute_fact_stored_procedures():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    ms_sql.execute_SP_insert_fact_availability_history()
    ms_sql.execute_SP_insert_fact_turnover_history()
    ms_sql.execute_SP_insert_update_fact_lease()
    ms_sql.execute_SP_insert_update_fact_lease_history()
    ms_sql.execute_SP_insert_update_fact_lease_all_units()
    ms_sql.execute_SP_insert_update_fact_lease_all_units_history()
    ms_sql.execute_SP_insert_update_fact_lease_expiration_renewal()
    ms_sql.execute_SP_insert_fact_weekly_reports()
    ms_sql.execute_SP_populate_income_budget_fact_table(type=1)
    ms_sql.execute_SP_populate_income_budget_fact_table(type=2)
    ms_sql.execute_SP_fact_delinquent_history()


def execute_dimensions_stored_procedures():
    ms_sql = MSSQL(server=os.getenv("SERVER_NAME"), database=os.getenv("DATABASE_NAME"))
    ms_sql.connect()
    #ms_sql.execute_SP_insert_update_Dim_info_property()
    ms_sql.execute_SP_insert_update_dim_unit()
    ms_sql.execute_SP_insert_dim_resident_demographics()
    #ms_sql.execute_SP_insert_update_dim_info_property_history()
    ms_sql.execute_SP_dim_income_budget_category_procedure()
    ms_sql.execute_SP_dim_requests()


def load_staging():
    #load_acquisition_entities_propertyinfo()
    #load_all_units_lease_deatails_lease_expiration()
    #load_availability_turnover()
    #process_finance_statements_to_sql()
    #load_weekly_files()
    #load_delinquent_files()
    #load_requests()
    #load_lease_report()
    #load_lease_trade_report()
    #load_google_reviews_report()
    #load_market_rate_report()
    #load_general_ledger_turn_report()
    #load_unit_amenities_report()
    #load_unit_rent_summary_report()
    #load_unit_turn_capx_report()
    #load_unit_rehab_report()
    #load_unit_standard_turn_report()
    #load_unit_renewal_offer_report()
    #load_lease_activity_detail_report()
    #load_unit_availbility_report()
    #unit_setup_view()
    load_service_requests_new()
    #load_resident_details_new()
    #load_soci_report_details()
    #load_soci_review_details()
    #load_knock_activity_report()
    #load_knock_conversion_report()
    #load_yelp_review_details()
    #load_ellis_bi_report_weekly()
    #load_ellis_bi_report_monthly()
    #load_ellis_KPI_report_weekly()
    #load_ellis_KPI_report_monthly()
    #load_ellis_survey_report()
    #load_knock_engagement_report()
    #load_rent_grata_unpaid_rewards_report()
    #load_rent_grata_weekly_report()
    #load_bi_banners_error_report()
    #load_availability_onetime()
    #load_lease_activity_new()
    #load_rent_grata_daily_report()
    #load_delinquent_files_pssql()
    #load_lease_activity_detail_report_psql()
    


def execute_stored_procedures():
    execute_dimensions_stored_procedures()
    execute_fact_stored_procedures()

def load_contact_level_details():
    
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


if __name__ == "__main__":
    try:
        load_staging()
        #execute_stored_procedures()
        #load_contact_level_details()
        logging.info("manual workflow was successful")

    except Exception as e:
        logging.error("Script failed to run ", exc_info=e)
