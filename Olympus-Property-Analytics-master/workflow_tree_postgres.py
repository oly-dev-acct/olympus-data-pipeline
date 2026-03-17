from prefect import flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
from prefect.filesystems import LocalFileSystem


from prefect_ps_sql_load import (
    load_acquisition_entities_propertyinfo_script,
    load_all_units_lease_deatails_lease_expiration_script,
    load_availability_turnover_script,
    execute_dimensions_stored_procedures,
    execute_fact_stored_procedures,
    process_finance_statements_to_sql,
    load_weekly_files_script,
    load_renewals_friday_reports_script,
    load_request_files_script,
    load_delinquent_files_script,
    load_lease_report_files_script,
    load_lease_tradeout_report_files_script,
    load_google_reviews_script,
    load_market_rate_report_script,
    load_unit_rent_summary_report_script,
    load_unit_amenities_report_script,
    load_unit_capx_rehab_standard_turn_script,
    load_unit_renewal_offer_report_script,
    load_unit_availbility_report_script,
    load_soci_report_details_script,
    load_service_requests_new_script,
    load_soci_review_details_new_script,
    load_knock_activity_report_new_script,
    load_knock_conversion_report_new_script,
    load_bi_ellis_loyality_report_new_script,
    load_bi_ellis_kpi_report_new_script,
    load_bi_weekly_rent_grata_report_new_script,
    load_rent_grata_unpaid_reward_report_new_script,
    load_bi_banners_new_script,
    load_knock_engagement_report_new_script,
    load_lease_activity_new_script,
    load_contact_level_details_script
)

# Load Prefect block
local_file_system_block = LocalFileSystem.load("olympus-property-analytics")


@flow(task_runner=ConcurrentTaskRunner())
def load_staging():
    
    load_acquisition_entities_propertyinfo_script.submit()  # Uncomment when needed
    load_all_units_lease_deatails_lease_expiration_script.submit()
    load_availability_turnover_script.submit()
    process_finance_statements_to_sql.submit()
    load_weekly_files_script.submit()
    load_renewals_friday_reports_script.submit()
    load_request_files_script.submit()
    load_delinquent_files_script.submit()
    load_lease_report_files_script.submit()
    load_lease_tradeout_report_files_script.submit()
    load_google_reviews_script.submit()
    load_market_rate_report_script.submit()
    load_unit_rent_summary_report_script.submit()
    load_unit_amenities_report_script.submit()
    load_unit_capx_rehab_standard_turn_script.submit()
    load_unit_renewal_offer_report_script.submit()
    load_unit_availbility_report_script.submit()
    load_soci_report_details_script.submit()
    load_service_requests_new_script.submit()
    load_soci_review_details_new_script.submit()
    load_knock_activity_report_new_script.submit()
    load_knock_conversion_report_new_script.submit()
    load_bi_ellis_loyality_report_new_script.submit()
    load_bi_ellis_kpi_report_new_script.submit()
    #load_bi_weekly_rent_grata_report_new_script.submit()
    load_rent_grata_unpaid_reward_report_new_script.submit()
    #load_bi_banners_new_script.submit()
    load_knock_engagement_report_new_script.submit()
    load_lease_activity_new_script.submit()
    load_contact_level_details_script.submit()


@flow(task_runner=SequentialTaskRunner())
def workflow_postgres():

    logger = get_run_logger()
    try:
        load_staging()
        execute_dimensions_stored_procedures()
        execute_fact_stored_procedures(return_state=True)
        logger.info("Flow completed successfully ✅")
    except Exception as e:
        logger.error("Workflow failed ❌", exc_info=e)


if __name__ == "__main__":
    workflow()
