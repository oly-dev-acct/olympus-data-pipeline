from database import PostgreSQL
import os
import logging
from dotenv import load_dotenv
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime

load_dotenv()


logging.basicConfig(
    filename="execute_stored_procedures.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)


@flow(name="execute_SP_insert_fact_availability_history")
def execute_SP_insert_fact_availability_history():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_fact_availability_history()


@flow(name="execute_SP_insert_fact_turnover_history")
def execute_SP_insert_fact_turnover_history():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_fact_turnover_history()


@flow(name="execute_SP_insert_update_fact_lease")
def execute_SP_insert_update_fact_lease():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease()


@flow(name="execute_SP_insert_update_fact_lease_history")
def execute_SP_insert_update_fact_lease_history():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease_history()


@flow(name="execute_SP_insert_update_fact_lease_all_units")
def execute_SP_insert_update_fact_lease_all_units():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease_all_units()


@flow(name="execute_SP_insert_update_fact_lease_all_units_history")
def execute_SP_insert_update_fact_lease_all_units_history():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease_all_units_history()


@flow(name="execute_SP_insert_fact_weekly_reports")
def execute_SP_insert_fact_weekly_reports():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_fact_weekly_reports()

@flow(name="execute_SP_insert_update_fact_lease_expiration_renewal")
def execute_SP_insert_update_fact_lease_expiration_renewal():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease_expiration_renewal()


@flow(name="execute_SP_populate_budget_fact_table")
def execute_SP_populate_budget_fact_table():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_populate_income_budget_fact_table(type=1)


@flow(name="execute_SP_populate_income_fact_table")
def execute_SP_populate_income_fact_table():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_populate_income_budget_fact_table(type=2)

@flow(name="execute_SP_fact_delinquent_history")
def execute_SP_fact_delinquent_history():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_fact_delinquent_history()

@flow(name="execute_SP_insert_update_fact_lease_tradeout_report")
def execute_SP_insert_update_fact_lease_tradeout_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease_tradeout_report()

@flow(name="execute_SP_insert_update_fact_lease_report")
def execute_SP_insert_update_fact_lease_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_fact_lease_report()

@flow(name="execute_SP_fact_insert_pending_renewals_history")
def execute_SP_fact_insert_pending_renewals_history():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_fact_insert_pending_renewals_history()

@flow(name="execute_sp_insert_update_google_reviews")
def execute_sp_insert_update_google_reviews():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_google_reviews()

@flow(name="execute_sp_insert_market_rent_report")
def execute_sp_insert_market_rent_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_market_rent_report()

@flow(name="execute_sp_insert_unit_amenities_hist")
def execute_sp_insert_unit_amenities_hist():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_unit_amenities_hist()

@flow(name="execute_sp_insert_update_unit_turn_capx_report")
def execute_sp_insert_update_unit_turn_capx_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_unit_turn_capx_report()

@flow(name="execute_sp_insert_update_unit_turn_rehab_report")
def execute_sp_insert_update_unit_turn_rehab_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_unit_turn_rehab_report()

@flow(name="execute_sp_insert_update_unit_turn_standard_report")
def execute_sp_insert_update_unit_turn_standard_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_unit_turn_standard_report()

@flow(name="execute_sp_insert_unit_renewal_offer_analysis_report")
def execute_sp_insert_unit_renewal_offer_analysis_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_unit_renewal_offer_analysis_report()

@flow(name="execute_sp_insert_update_lease_activity_detail")
def execute_sp_insert_update_lease_activity_detail():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_lease_activity_detail()

@flow(name="execute_sp_soci_report_details_snapshot")
def execute_sp_soci_report_details_snapshot():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_soci_report_details_snapshot()

@flow(name="execute_sp_insert_dim_requests_new")
def execute_sp_insert_dim_requests_new():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_dim_requests_new()

@flow(name="execute_sp_insert_update_soci_review_feed_details")
def execute_sp_insert_update_soci_review_feed_details():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_soci_review_feed_details()

@flow(name="sp_insert_knock_activity_weekly_snapshot")
def execute_sp_insert_knock_activity_weekly_snapshot():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_knock_activity_weekly_snapshot()

@flow(name="execute_sp_insert_knock_activity_daily_snapshot")
def execute_sp_insert_knock_activity_daily_snapshot():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_knock_activity_daily_snapshot()

@flow(name="execute_sp_insert_knock_conversion_daily_snapshot")
def execute_sp_insert_knock_conversion_daily_snapshot():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_knock_conversion_daily_snapshot()

@flow(name="execute_sp_insert_update_ellis_weekly_monthly_KPI")
def execute_sp_insert_update_ellis_weekly_monthly_KPI():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_ellis_weekly_monthly_KPI()

@flow(name="execute_sp_insert_update_ellis_weekly_monthly_loyality")
def execute_sp_insert_update_ellis_weekly_monthly_loyality():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_ellis_weekly_monthly_loyality()

@flow(name="execute_sp_insert_update_weekly_rent_grata_report")
def execute_sp_insert_update_weekly_rent_grata_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_weekly_rent_grata_report()

@flow(name="execute_sp_insert_update_weekly_rent_grata_unpaid_rewards")
def execute_sp_insert_update_weekly_rent_grata_unpaid_rewards():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_weekly_rent_grata_unpaid_rewards()

@flow(name="execute_sp_insert_knock_engagement_weekly_snapshot")
def execute_sp_insert_knock_engagement_weekly_snapshot():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_knock_engagement_weekly_snapshot()

@flow(name="execute_sp_insert_update_bi_banner_report")
def execute_sp_insert_update_bi_banner_report():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_bi_banner_report()

@flow(name="execute_sp_insert_update_lease_activty_new")
def execute_sp_insert_update_lease_activty_new():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_insert_update_lease_activty_new()


@flow(name="execute_sp_refresh_ps_sql_materlalized_views")
def execute_sp_refresh_ps_sql_materlalized_views():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_sp_refresh_ps_sql_materlalized_views()

@flow(name="execute_dimensions_stored_procedures")
def execute_dimensions_stored_procedures():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_insert_update_Dim_info_property()
    ps_sql.execute_SP_insert_update_dim_unit()
    #ps_sql.execute_SP_insert_dim_resident_demographics()
    #ps_sql.execute_SP_insert_update_dim_info_property_history()
    ps_sql.execute_SP_dim_income_budget_category_procedure()
    ps_sql.execute_SP_dim_requests()
    ps_sql.execute_sp_contact_level_details()

@flow(name="execute_SP_olympus_lease_trend_analysis_procedure")
def execute_SP_olympus_lease_trend_analysis_procedure():
    today_date = datetime.now().strftime("%Y-%m-%d")
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.execute_SP_olympus_lease_trend_analysis_procedure(start_date=today_date
                                                             , end_date=today_date)


@flow(name="execute_fact_stored_procedures", task_runner=ConcurrentTaskRunner())
def execute_fact_stored_procedures():
    execute_SP_insert_fact_availability_history(return_state=True)
    execute_SP_insert_fact_turnover_history(return_state=True)
    execute_SP_insert_update_fact_lease(return_state=True)
    #execute_SP_insert_update_fact_lease_history(return_state=True)
    execute_SP_insert_update_fact_lease_all_units(return_state=True)
    #execute_SP_insert_update_fact_lease_all_units_history(return_state=True)
    execute_SP_insert_update_fact_lease_expiration_renewal(return_state=True)
    execute_SP_populate_budget_fact_table(return_state=True)
    execute_SP_populate_income_fact_table(return_state=True)
    execute_SP_insert_fact_weekly_reports(return_state=True)
    #execute_SP_fact_insert_pending_renewals_history(return_state=True)
    execute_SP_fact_delinquent_history(return_state=True)
    execute_SP_insert_update_fact_lease_report(return_state=True)
    execute_SP_insert_update_fact_lease_tradeout_report(return_state=True)
    execute_sp_insert_market_rent_report(return_state=True)
    execute_sp_insert_unit_amenities_hist(return_state=True)
    execute_sp_insert_update_unit_turn_capx_report(return_state=True)
    execute_sp_insert_update_unit_turn_rehab_report(return_state=True)
    execute_sp_insert_update_unit_turn_standard_report(return_state=True)
    execute_sp_insert_unit_renewal_offer_analysis_report(return_state=True)
    execute_sp_insert_update_lease_activity_detail(return_state=True)
    #execute_sp_soci_report_details_snapshot(return_state=True)
    execute_sp_insert_dim_requests_new(return_state=True)
    execute_sp_insert_update_soci_review_feed_details(return_state=True)
    execute_sp_insert_knock_activity_daily_snapshot(return_state=True)
    execute_sp_insert_knock_conversion_daily_snapshot(return_state=True)
    execute_SP_olympus_lease_trend_analysis_procedure(return_state=True)
    #execute_sp_insert_update_google_reviews(return_state=True)
    execute_sp_insert_update_ellis_weekly_monthly_loyality(return_state=True)
    execute_sp_insert_update_ellis_weekly_monthly_KPI(return_state=True)
    #execute_sp_insert_update_weekly_rent_grata_report(return_state=True)
    #execute_sp_insert_update_weekly_rent_grata_unpaid_rewards(return_state=True)
    execute_sp_insert_knock_engagement_weekly_snapshot(return_state=True)
    #execute_sp_insert_update_bi_banner_report(return_state=True)
    execute_sp_insert_update_lease_activty_new(return_state=True)
    execute_sp_insert_knock_activity_weekly_snapshot(return_state=True)
    execute_sp_refresh_ps_sql_materlalized_views(return_state=True)
