import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import io

class PostgreSQL:

    def __init__(self, host, database, user, password, port=5432, schema=None):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.schema = schema
        self.engine = None

    def connect(self, echo=False):
        connection_string = (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        self.engine = create_engine(connection_string, pool_pre_ping=True, echo=echo)
        logging.info("Connected to the PostgreSQL database.")

    def execute_query(self, query, params=None):
        """
        Execute a SQL statement. Returns:
          - list[dict]  if rows are returned
          - {"rowcount": n} otherwise
        """

        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            if result.returns_rows:
                columns = result.keys()
                rows = result.fetchall()
                # return list of dicts for convenience
                return [dict(zip(columns, r)) for r in rows]
            else:
                return {"rowcount": result.rowcount}

    def insert_dataframe(self, table_name, df: pd.DataFrame, batch_size: int = 10000, if_exists="append", index=False, schema=None):
        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        if schema is None:
            schema = self.schema

        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=index,
            method="multi",
            chunksize=batch_size,
            schema=schema,
        )
        logging.info(f"Inserted dataframe into {table_name} (if_exists={if_exists}).")

    def insert_dataframe_copy_command_contact_details(self, table_name, df: pd.DataFrame, batch_size: int = 10000, index=False, schema=None):
        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        if schema is None:
            schema = self.schema

        full_table_name = f"{schema}.{table_name}" if schema else table_name

        conn = self.engine.raw_connection()
        cursor = conn.cursor()
        try:

            buffer = io.StringIO()
            import csv

            cols = ",".join(df.columns)
            
            df.to_csv(buffer, index=False,header=False, quoting=csv.QUOTE_ALL, quotechar='"', escapechar='\\')

            buffer.seek(0)

            cursor.copy_expert(
                 f"COPY {full_table_name} ({cols}) FROM STDIN WITH CSV QUOTE '\"' ESCAPE '\\'",
            
                buffer
            )
            conn.commit()
            logging.info(f"Inserted {len(df)} rows into {full_table_name} using COPY.")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error inserting dataframe into {full_table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def insert_dataframe_copy_command(self, table_name, df: pd.DataFrame, batch_size: int = 10000, index=False, schema=None):
        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        if schema is None:
            schema = self.schema

        full_table_name = f"{schema}.{table_name}" if schema else table_name

        conn = self.engine.raw_connection()
        cursor = conn.cursor()
        try:

            buffer = io.StringIO()
            df.to_csv(buffer, index=index, header=False)  
            buffer.seek(0)

            cursor.copy_expert(
                f"COPY {full_table_name} FROM STDIN WITH CSV",
                buffer
            )
            conn.commit()
            logging.info(f"Inserted {len(df)} rows into {full_table_name} using COPY.")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error inserting dataframe into {full_table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def call_procedure(self, proc_name, *args, **kwargs):
        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        params = {}
        placeholders = []

 
        for i, v in enumerate(args):
            p = f"arg{i}"
            params[p] = v
            placeholders.append(f":{p}")

        for k, v in kwargs.items():
            params[k] = v
            placeholders.append(f":{k}")

        placeholder_str = ", ".join(placeholders)
        query = f"CALL staging.{proc_name}({placeholder_str})"
        logging.debug("Calling procedure: %s with params: %s", proc_name, params)
        return self.execute_query(query, params)

    # Stored-procedure wrapper methods 
    def execute_SP_Remove_OldData_And_Duplicates(self, days, type):
        res = self.call_procedure("SP_Remove_OldData_And_Duplicates", days, type)
        logging.info("stored procedure SP_Remove_OldData_And_Duplicates executed successfully")
        return res

    def execute_SP_populate_income_budget_fact_table(self, type):
        res = self.call_procedure("sp_populate_income_budget_fact_table", type)
        logging.info("stored procedure SP_populate_income_budget_fact_table executed successfully")
        return res

    def execute_SP_insert_fact_availability_history(self):
        res = self.call_procedure("sp_insert_fact_availability_history")
        logging.info("Stored Procedure SP_insert_fact_availability_history executed successfully")
        return res

    def execute_SP_insert_fact_turnover_history(self):
        res = self.call_procedure("sp_insert_fact_turnover_history")
        logging.info("Stored Procedure SP_insert_fact_turnover_history executed successfully")
        return res

    def execute_SP_insert_update_dim_unit(self):
        res = self.call_procedure("sp_insert_dim_unit")
        logging.info("Stored Procedure SP_insert_dim_unit executed successfully")
        return res

    def execute_SP_dim_income_budget_category_procedure(self):
        res = self.call_procedure("sp_populate_income_budget_categories")
        logging.info("Stored Procedure sp_populate_income_budget_categories executed successfully")
        return res

    def execute_SP_insert_dim_resident_demographics(self):
        res = self.call_procedure("sp_insert_dim_resident_demographics")
        logging.info("Stored Procedure SP_insert_dim_resident_demographics executed successfully")
        return res

    def execute_SP_insert_update_Dim_info_property(self):
        res = self.call_procedure("sp_insert_update_Dim_info_property")
        logging.info("Stored Procedure SP_insert_update_Dim_info_property executed successfully")
        return res

    def execute_SP_insert_update_dim_info_property_history(self):
        res = self.call_procedure("sp_insert_update_dim_info_property_history")
        logging.info("Stored Procedure SP_insert_update_dim_info_property_history executed successfully")
        return res

    def execute_SP_insert_update_fact_lease(self):
        res = self.call_procedure("sp_insert_update_fact_lease")
        logging.info("Stored Procedure SP_insert_update_fact_lease executed successfully")
        return res

    def execute_SP_insert_update_fact_lease_all_units(self):
        res = self.call_procedure("sp_insert_update_fact_lease_all_units")
        logging.info("Stored Procedure SP_insert_update_fact_lease_all_units executed successfully")
        return res

    def execute_SP_insert_update_fact_lease_expiration_renewal(self):
        res = self.call_procedure("sp_insert_update_fact_lease_expiration_renewal")
        logging.info("Stored Procedure SP_insert_update_fact_lease_expiration_renewal executed successfully")
        return res

    def execute_SP_populate_dim_date(self, startyear, endyear):
        res = self.call_procedure("SP_POPULATE_DIM_DATE", startyear, endyear)
        logging.info("Stored Procedure SP_POPULATE_DIM_DATE executed successfully")
        return res

    def execute_SP_insert_update_fact_lease_history(self):
        res = self.call_procedure("sp_insert_update_fact_lease_history")
        logging.info("Stored Procedure SP_insert_update_fact_lease_history executed successfully")
        return res

    def execute_SP_insert_update_fact_lease_all_units_history(self):
        res = self.call_procedure("sp_insert_update_fact_lease_all_units_history")
        logging.info("Stored Procedure SP_insert_update_fact_lease_all_units_history executed successfully")
        return res

    def execute_SP_insert_fact_weekly_reports(self):
        res = self.call_procedure("sp_insert_fact_weekly_reports")
        logging.info("Stored Procedure SP_insert_fact_weekly_reports executed successfully")
        return res

    def execute_SP_dim_requests(self):
        res = self.call_procedure("sp_dim_requests")
        logging.info("Stored Procedure sp_dim_requests executed successfully")
        return res

    def execute_SP_fact_delinquent_history(self):
        res = self.call_procedure("sp_fact_delinquent_history")
        logging.info("Stored Procedure SP_fact_delinquent_history executed successfully")
        return res

    def execute_SP_fact_insert_pending_renewals_history(self):
        res = self.call_procedure("sp_insert_pending_renewals_history")
        logging.info("Stored Procedure SP_insert_pending_renewals_history executed successfully")
        return res

    def execute_SP_insert_update_fact_lease_tradeout_report(self):
        res = self.call_procedure("sp_insert_update_fact_leasetradeout_report")
        logging.info("Stored Procedure SP_insert_update_fact_lease_tradeout_report executed successfully")
        return res

    def execute_SP_insert_update_fact_lease_report(self):
        res = self.call_procedure("sp_insert_update_fact_lease_report")
        logging.info("Stored Procedure SP_insert_update_fact_lease_report executed successfully")
        return res

    def execute_SP_olympus_lease_trend_analysis_procedure(self, start_date, end_date):
        res = self.call_procedure("sp_olympus_lease_trend_analysis_procedure", start_date, end_date)
        logging.info("Stored Procedure SP_olympus_lease_trend_analysis_procedure executed successfully")
        return res

    def execute_sp_insert_update_google_reviews(self):
        res = self.call_procedure("sp_insert_update_google_reviews")
        logging.info("Stored Procedure sp_insert_update_google_reviews executed successfully")
        return res

    def execute_sp_insert_market_rent_report(self):
        res = self.call_procedure("sp_insert_market_rent_report")
        logging.info("Stored Procedure sp_insert_market_rent_report executed successfully")
        return res

    def execute_sp_insert_unit_amenities_hist(self):
        res = self.call_procedure("sp_insert_unit_amenities_hist")
        logging.info("Stored Procedure sp_insert_unit_amenities_hist executed successfully")
        return res

    def execute_sp_insert_update_unit_turn_capx_report(self):
        res = self.call_procedure("sp_insert_update_unit_turn_capx_report")
        logging.info("Stored Procedure sp_insert_update_unit_turn_capx_report executed successfully")
        return res

    def execute_sp_insert_update_unit_turn_rehab_report(self):
        res = self.call_procedure("sp_insert_update_unit_turn_rehab_report")
        logging.info("Stored Procedure sp_insert_update_unit_turn_rehab_report executed successfully")
        return res

    def execute_sp_insert_update_unit_turn_standard_report(self):
        res = self.call_procedure("sp_insert_update_unit_turn_standard_report")
        logging.info("Stored Procedure sp_insert_update_unit_turn_standard_report executed successfully")
        return res

    def execute_sp_insert_unit_renewal_offer_analysis_report(self):
        res = self.call_procedure("sp_insert_unit_renewal_offer_analysis_report")
        logging.info("Stored Procedure sp_insert_unit_renewal_offer_analysis_report executed successfully")
        return res

    def execute_sp_insert_update_lease_activity_detail(self):
        res = self.call_procedure("sp_insert_update_lease_activity_detail")
        logging.info("Stored Procedure sp_insert_update_lease_activity_detail executed successfully")
        return res

    def execute_sp_soci_report_details_snapshot(self):
        res = self.call_procedure("sp_soci_report_details_snapshot")
        logging.info("Stored Procedure sp_soci_report_details_snapshot executed successfully")
        return res

    def execute_sp_insert_dim_requests_new(self):
        res = self.call_procedure("sp_insert_dim_requests_new")
        logging.info("Stored Procedure sp_insert_dim_requests_new executed successfully")
        return res

    def execute_sp_insert_update_soci_review_feed_details(self):
        res = self.call_procedure("sp_insert_update_soci_review_feed_details")
        logging.info("Stored Procedure sp_insert_update_soci_review_feed_details executed successfully")
        return res

    def execute_sp_insert_knock_activity_daily_snapshot(self):
        res = self.call_procedure("sp_insert_knock_activity_daily_snapshot")
        logging.info("Stored Procedure sp_insert_knock_activity_daily_snapshot executed successfully")
        return res

    def execute_sp_insert_knock_activity_weekly_snapshot(self):
        res = self.call_procedure("sp_insert_knock_activity_weekly_snapshot")
        logging.info("Stored Procedure sp_insert_knock_activity_weekly_snapshot executed successfully")
        return res

    def execute_sp_insert_knock_conversion_daily_snapshot(self):
        res = self.call_procedure("sp_insert_knock_conversion_daily_snapshot")
        logging.info("Stored Procedure sp_insert_knock_conversion_daily_snapshot executed successfully")
        return res

    def execute_sp_insert_update_ellis_weekly_monthly_KPI(self):
        res = self.call_procedure("sp_insert_update_ellis_weekly_monthly_KPI")
        logging.info("Stored Procedure sp_insert_update_ellis_weekly_monthly_KPI executed successfully")
        return res

    def execute_sp_insert_update_ellis_weekly_monthly_loyality(self):
        res = self.call_procedure("sp_insert_update_ellis_weekly_monthly_loyality")
        logging.info("Stored Procedure sp_insert_update_ellis_weekly_monthly_loyality executed successfully")
        return res

    def execute_sp_insert_update_weekly_rent_grata_report(self):
        res = self.call_procedure("sp_insert_update_weekly_rent_grata_report")
        logging.info("Stored Procedure sp_insert_update_weekly_rent_grata_report executed successfully")
        return res

    def execute_sp_insert_update_weekly_rent_grata_unpaid_rewards(self):
        res = self.call_procedure("sp_insert_update_weekly_rent_grata_unpaid_rewards")
        logging.info("Stored Procedure sp_insert_update_weekly_rent_grata_unpaid_rewards executed successfully")
        return res

    def execute_sp_insert_knock_engagement_weekly_snapshot(self):
        res = self.call_procedure("sp_insert_knock_engagement_weekly_snapshot")
        logging.info("Stored Procedure sp_insert_knock_engagement_weekly_snapshot executed successfully")
        return res

    def execute_sp_insert_update_bi_banner_report(self):
        res = self.call_procedure("sp_insert_update_bi_banner_report")
        logging.info("Stored Procedure sp_insert_update_bi_banner_report executed successfully")
        return res

    def execute_sp_insert_update_lease_activty_new(self):
        res = self.call_procedure("sp_insert_update_lease_activty_new")
        logging.info("Stored Procedure sp_insert_update_lease_activty_new executed successfully")
        return res

    def execute_sp_refresh_ps_sql_materlalized_views(self):
        res = self.call_procedure("refresh_pbi_web_ai_views")
        logging.info("Stored Procedure refresh_pbi_web_ai_views executed successfully")
        return res

    def execute_sp_contact_level_details(self):
        res = self.call_procedure("sp_contact_level_details")
        logging.info("Stored Procedure sp_contact_level_details executed successfully")
        return res

 
    def truncate_tables(self, table_list):
        if self.engine is None:
            logging.error("Database connection not established.")
            return None

        inspector = inspect(self.engine)
        existing = inspector.get_table_names(schema=self.schema) if self.schema else inspector.get_table_names()
        for table_name in table_list:
            if table_name in existing:
                full_table = f'"{table_name}"' if not self.schema else f'"{self.schema}"."{table_name}"'
                truncate_statement = f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE"
                self.execute_query(truncate_statement)
                logging.info("Truncated table %s", full_table)
            else:
                logging.warning("Table %s not found in database (skipped).", table_name)

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logging.info("Database engine disposed.")
