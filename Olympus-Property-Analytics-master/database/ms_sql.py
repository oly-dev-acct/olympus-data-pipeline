import logging
import numpy as np
import logging
from sqlalchemy import create_engine, text, inspect


class MSSQL:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.engine = None
        self.connection = None
        self.cursor = None

    def connect(self):
        #connection_string = f"mssql+pyodbc://{self.server}/?database={self.database}&driver=SQL+Server&trusted_connection=yes"
        connection_string = f"mssql+pyodbc://{self.server}/?database={self.database}&driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"


        self.engine = create_engine(
            connection_string,
            use_setinputsizes=False,
            fast_executemany=True
        )
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor()
        logging.info("Connected to the MS SQL Server database.")

    def execute_query(self, query):
        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        result = self.connection.execute(str(query))
        logging.info("Query executed successfully.")
        self.connection.commit()
        return result

    def insert_dataframe(self, table_name, df):
        batch_size = 10000
        num_batches = int(np.ceil(len(df) / batch_size))
        for batch_index in range(num_batches):
            batch_start = batch_index * batch_size
            batch_end = (batch_index + 1) * batch_size
            batch_df = df.iloc[batch_start:batch_end]

            batch_df.to_sql(table_name, self.engine, if_exists="append", index=False)

    def execute_SP_Remove_OldData_And_Duplicates(self, days, type):
        query = text(
            f"""
                        DECLARE @RowCountOutput INT
                        EXEC SP_Remove_OldData_And_Duplicates @NumberOfDays = {days}, @Type={type}, @ROWCOUNT = @RowCountOutput OUTPUT
                        SELECT @RowCountOutput AS [RowCountOutput]
                                        
                                                                                                    """
        )
        self.execute_query(query=query)
        logging.info(
            f"stored procedure  SP_Remove_OldData_And_Duplicates executed successfully "
        )

    def execute_SP_populate_income_budget_fact_table(self, type):
        query = text(f"exec SP_populate_income_budget_fact_table  @Type={type}")
        result = self.execute_query(query=query)

        logging.info(
            f"stored procedure  SP_populate_income_budget_fact_table executed successfully "
        )

    def execute_SP_insert_fact_availability_history(self):
        query = text(f"EXEC SP_insert_fact_availability_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_fact_availability_history executed successfully"
        )

    def execute_SP_insert_fact_turnover_history(self):
        query = text("EXEC SP_insert_fact_turnover_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_fact_turnover_history executed successfully"
        )

    def execute_SP_insert_update_dim_unit(self):
        query = text("EXEC SP_insert_dim_unit")
        self.execute_query(query=query)
        logging.info("Stored Procedure SP_insert_dim_unit executed successfully")

    def execute_SP_dim_income_budget_category_procedure(self):
        query = text("EXEC sp_populate_income_budget_categories")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure sp_populate_income_budget_categories executed successfully"
        )

    def execute_SP_insert_dim_resident_demographics(self):
        query = text("EXEC SP_insert_dim_resident_demographics")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_dim_resident_demographics executed successfully"
        )

    def execute_SP_insert_update_Dim_info_property(self):
        query = text("EXEC SP_insert_update_Dim_info_property")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_Dim_info_property executed successfully"
        )

    def execute_SP_insert_update_dim_info_property_history(self):
        query = text("EXEC SP_insert_update_dim_info_property_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_dim_info_property_history executed successfully"
        )

    def execute_SP_insert_update_fact_lease(self):
        query = text("EXEC SP_insert_update_fact_lease")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease executed successfully"
        )

    def execute_SP_insert_update_fact_lease_all_units(self):
        query = text("EXEC SP_insert_update_fact_lease_all_units")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease_all_units executed successfully"
        )

    def execute_SP_insert_update_fact_lease_expiration_renewal(self):
        query = text("EXEC SP_insert_update_fact_lease_expiration_renewal")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease_expiration_renewal executed successfully"
        )

    def execute_SP_populate_dim_date(self, startyear, endyear):
        query = text(
            f"EXEC SP_POPULATE_DIM_DATE  @StartYear={startyear} ,@EndYear={endyear} "
        )
        self.execute_query(query=query)
        logging.info("Stored Procedure SP_POPULATE_DIM_DATE executed successfully")

    def execute_SP_insert_update_fact_lease_history(self):
        query = text("EXEC SP_insert_update_fact_lease_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease_history executed successfully"
        )

    def execute_SP_insert_update_fact_lease_all_units_history(self):
        query = text("EXEC SP_insert_update_fact_lease_all_units_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease_all_units_history executed successfully"
        )
    def execute_SP_insert_fact_weekly_reports(self):
        query = text("EXEC SP_insert_fact_weekly_reports")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_fact_weekly_reports"
        )
    def execute_SP_dim_requests(self):
        query = text("EXEC SP_dim_requests")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_dim_requests executed successfully"
        )
    def execute_SP_fact_delinquent_history(self):
        query = text(f"EXEC SP_fact_delinquent_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_fact_delinquent_history executed successfully"
     
        )
    def execute_SP_fact_insert_pending_renewals_history(self):
        query = text("EXEC SP_insert_pending_renewals_history")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_pending_renewals_history executed successfully"
        )

    def execute_SP_insert_update_fact_lease_tradeout_report(self):
        query = text(f"EXEC SP_insert_update_fact_lease_tradeout_report")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease_tradeout_report executed successfully"
        )

    def execute_SP_insert_update_fact_lease_report(self):
        query = text(f"EXEC SP_insert_update_fact_lease_report")
        self.execute_query(query=query)
        logging.info(
            "Stored Procedure SP_insert_update_fact_lease_report executed successfully"
        )

    def execute_SP_olympus_lease_trend_analysis_procedure(self, start_date, end_date):
        query = text(f"EXEC SP_olympus_lease_trend_analysis_procedure  @startdate = '{start_date}', @endDate = '{end_date}'")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure SP_olympus_lease_trend_analysis_procedure executed successfully "
        )

    def execute_sp_insert_update_google_reviews(self):
        query = text(f"EXEC sp_insert_update_google_reviews")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_google_reviews executed successfully "
        )

    def execute_sp_insert_market_rent_report(self):
        query = text(f"EXEC sp_insert_market_rent_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_market_rent_report executed successfully "
        )

    def execute_sp_insert_unit_amenities_hist(self):
        query = text(f"EXEC sp_insert_unit_amenities_hist")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_unit_amenities_hist executed successfully "
        )

    def execute_sp_insert_update_unit_turn_capx_report(self):
        query = text(f"EXEC sp_insert_update_unit_turn_capx_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_update_unit_turn_capx_report executed successfully "
        )

    def execute_sp_insert_update_unit_turn_rehab_report(self):
        query = text(f"EXEC sp_insert_update_unit_turn_rehab_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_update_unit_turn_rehab_report executed successfully "
        )

    def execute_sp_insert_update_unit_turn_standard_report(self):
        query = text(f"EXEC sp_insert_update_unit_turn_standard_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_update_unit_turn_standard_report executed successfully "
        )

    def execute_sp_insert_unit_renewal_offer_analysis_report(self):
        query = text(f"EXEC sp_insert_unit_renewal_offer_analysis_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_unit_renewal_offer_analysis_report executed successfully "
        )

    def execute_sp_insert_update_lease_activity_detail(self):
        query = text(f"EXEC sp_insert_update_lease_activity_detail")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_update_lease_activity_detail executed successfully "
        )

    def execute_sp_soci_report_details_snapshot(self):
        query = text(f"EXEC sp_soci_report_details_snapshot")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_soci_report_details_snapshot executed successfully "
        )

    def execute_sp_insert_dim_requests_new(self):
        query = text(f"EXEC sp_insert_dim_requests_new")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_dim_requests_new executed successfully "
        )

    def execute_sp_insert_update_soci_review_feed_details(self):
        query = text(f"EXEC sp_insert_update_soci_review_feed_details")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_update_soci_review_feed_details executed successfully "
        )

    def execute_sp_insert_knock_activity_daily_snapshot(self):
        query = text(f"EXEC sp_insert_knock_activity_daily_snapshot")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure execute_sp_insert_knock_activity_daily_snapshot executed successfully "
        )

    def execute_sp_insert_knock_activity_weekly_snapshot(self):
        query = text(f"EXEC sp_insert_knock_activity_weekly_snapshot")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_knock_activity_weekly_snapshot executed successfully "
        )

    def execute_sp_insert_knock_conversion_daily_snapshot(self):
        query = text(f"EXEC sp_insert_knock_conversion_daily_snapshot")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_knock_conversion_daily_snapshot executed successfully "
        )

    def execute_sp_insert_update_ellis_weekly_monthly_KPI(self):
        query = text(f"EXEC sp_insert_update_ellis_weekly_monthly_KPI")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_ellis_weekly_monthly_KPI executed successfully "
        )

    def execute_sp_insert_update_ellis_weekly_monthly_loyality(self):
        query = text(f"EXEC sp_insert_update_ellis_weekly_monthly_loyality")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_ellis_weekly_monthly_loyality executed successfully "
        )

    def execute_sp_insert_update_weekly_rent_grata_report(self):
        query = text(f"EXEC sp_insert_update_weekly_rent_grata_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_weekly_rent_grata_report executed successfully "
        )

    def execute_sp_insert_update_weekly_rent_grata_unpaid_rewards(self):
        query = text(f"EXEC sp_insert_update_weekly_rent_grata_unpaid_rewards")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_weekly_rent_grata_unpaid_rewards executed successfully "
        )

    def execute_sp_insert_knock_engagement_weekly_snapshot(self):
        query = text(f"EXEC sp_insert_knock_engagement_weekly_snapshot")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_knock_engagement_weekly_snapshot executed successfully "
        )

    def execute_sp_insert_update_bi_banner_report(self):
        query = text(f"EXEC sp_insert_update_bi_banner_report")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_bi_banner_report executed successfully "
        )

    def execute_sp_insert_update_lease_activty_new(self):
        query = text(f"EXEC sp_insert_update_lease_activty_new")
        self.execute_query(query=query)
        logging.info(
            f"Stored Procedure sp_insert_update_lease_activty_new executed successfully "
        )
        
    def truncate_tables(self, table_list):
        inspector = inspect(self.engine)
        for table_name in table_list:
            if table_name in inspector.get_table_names():
                truncate_statement = text(f"TRUNCATE TABLE {table_name}")
                self.execute_query(truncate_statement)
