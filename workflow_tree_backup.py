from load_acquisition_entities_propertyinfo import (
    main as load_acquisition_entities_propertyinfo_script,
)

from load_all_units_lease_deatails_lease_expiration import (
    main as load_all_units_lease_deatails_lease_expiration_script,
)
from load_availability_turnover import main as load_availability_turnover_script
from execute_stored_procedures import (
    execute_dimensions_stored_procedures,
    execute_fact_stored_procedures,
)
from process_finance_statements_to_sql import main as process_finance_statements_to_sql
from load_weekly_files import main as load_weekly_files_script
from load_renewals_friday_reports import main as load_renewals_friday_reports_script
from load_requests_files import main as load_request_files_script
from load_delinquent import main as load_delinquent_files_script
from load_lease_report import main as load_lease_report_files_script
from load_lease_tradeout_report import main as load_lease_tradeout_report_files_script
from load_google_reviews import main as load_google_reviews_script
from load_market_rate_report import main as load_market_rate_report_script
from load_unit_rent_summary_report import main as load_unit_rent_summary_report_script
from load_unit_amenities_report import main as load_unit_amenities_report_script
from load_unit_capx_rehab_standard_turn import main as load_unit_capx_rehab_standard_turn_script
from load_unit_renewal_offer_report import main as load_unit_renewal_offer_report_script
from load_unit_availbility_report import main as load_unit_availbility_report_script
from load_soci_report_details import main as load_soci_report_details_script
from load_service_requests_new import main as load_service_requests_new_script
from load_soci_review_details import main as load_soci_review_details_new_script
from load_knock_activity_report import main as load_knock_activity_report_new_script
from load_knock_conversion_report import main as load_knock_conversion_report_new_script
from load_bi_ellis_loyality_report import main as load_bi_ellis_loyality_report_new_script
from load_bi_ellis_kpi_report import main as load_bi_ellis_kpi_report_new_script
from load_bi_weekly_rent_grata_report import main as load_bi_weekly_rent_grata_report_new_script
from load_rent_grata_unpaid_reward_report import main as load_rent_grata_unpaid_reward_report_new_script
from load_knock_engagement_report import main as load_knock_engagement_report_new_script
from load_bi_banners import main as load_bi_banners_new_script
from load_lease_activity_new import main as load_lease_activity_new_script
from prefect import flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
from prefect.filesystems import LocalFileSystem

local_file_system_block = LocalFileSystem.load("olympus-property-analytics")


@flow(task_runner=ConcurrentTaskRunner())
def load_staging():

    #load_acquisition_entities_propertyinfo_script.submit()
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
    


@flow(task_runner=SequentialTaskRunner())
def workflow():
    logger = get_run_logger()
    try:
        load_staging()
        execute_dimensions_stored_procedures()
        execute_fact_stored_procedures(return_state=True)
        logger.info("flow was successful")

    except Exception as e:
        logger.error("Script failed to run ", exc_info=e)


if __name__ == "__main__":
    workflow()
