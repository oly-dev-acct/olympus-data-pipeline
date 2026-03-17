import os
from dotenv import load_dotenv
from transform_data import (BIBannersErrors,
    BIBannersPendingInvoice,
    BIBannersAssignedInvoice,
    BIBannersDVInvoice)
import logging
from database import PostgreSQL
import time
from prefect import task

logging.basicConfig(
    filename="load_bi_banners_report_postgres.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)

load_dotenv()

@task(name="load_bi_banners_report_postgres")
def main():
    ps_sql = PostgreSQL(host=os.getenv("PS_SERVER_NAME"), database=os.getenv("PS_DATABASE_NAME"),user=os.getenv("PS_USER_NAME"), password=os.getenv("PS_PASSWORD"), schema=os.getenv("PS_SCHEMA"))
    ps_sql.connect()
    ps_sql.truncate_tables(
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
    ps_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_TABLE_NAME"),
        bi_banner_error.df,
    )

    bi_banner_pending_invoice = BIBannersPendingInvoice(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_pending_invoice.structurize_data()
    ps_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_PENDING_INVOICES_TABLE_NAME"),
        bi_banner_pending_invoice.df,
    )

    bi_banner_assigned_invoice = BIBannersAssignedInvoice(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_assigned_invoice.structurize_data()
    ps_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_ASSIGNED_INVOICES_TABLE_NAME"),
        bi_banner_assigned_invoice.df,
    )

    bi_banner_dv_invoice = BIBannersDVInvoice(os.getenv("BI_BANNER_ERROR_FOLDER_PATH"))
    bi_banner_dv_invoice.structurize_data()
    ps_sql.insert_dataframe(
        os.getenv("BI_BANNER_ERROR_DV_INVOICES_TABLE_NAME"),
        bi_banner_dv_invoice.df,
    )
    elapsed_time = time.time() - start_time
    logging.info(
        f'Sucessfully staged all data to {os.getenv("BI_BANNER_ERROR_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_PENDING_INVOICES_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_ASSIGNED_INVOICES_TABLE_NAME")}, {os.getenv("BI_BANNER_ERROR_DV_INVOICES_TABLE_NAME")} tables: Elapsed time {elapsed_time:.2f} seconds'
    )
