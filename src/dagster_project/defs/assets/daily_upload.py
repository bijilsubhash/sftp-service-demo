import dagster as dg
from datetime import datetime

from main import upload_data
from sftp.services.faker_service import FakerService
from dagster_project.defs.partitions import daily_partition
from dagster_project.constants import (
    ORDER_FILE_NAME,
    CUSTOMER_FILE_NAME,
    PRODUCT_FILE_NAME,
)


@dg.asset(partitions_def=daily_partition)
def order(context: dg.AssetExecutionContext) -> None:
    """Generate fake order data and upload to SFTP server for date."""
    date_formatted = datetime.strptime(context.partition_key, "%Y-%m-%d").strftime(
        "%d-%m-%Y"
    )
    faker_service = FakerService(date_formatted)
    faker_service.generate_data(size=1000)
    upload_data(ORDER_FILE_NAME, faker_service.output_dir)


@dg.asset
def customer() -> None:
    """Load customer data and upload to SFTP server."""
    upload_data(CUSTOMER_FILE_NAME)


@dg.asset
def product() -> None:
    """Load product data and upload to SFTP server."""
    upload_data(PRODUCT_FILE_NAME)
