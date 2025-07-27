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


@dg.asset(partitions_def=daily_partition, group_name="sftp")
def order(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Generate fake order data and upload to SFTP server for date."""
    date_formatted = datetime.strptime(context.partition_key, "%Y-%m-%d").strftime(
        "%d-%m-%Y"
    )
    faker_service = FakerService(date_formatted)
    faker_service.generate_data(size=1000)
    row_count = upload_data(ORDER_FILE_NAME, faker_service.output_dir)
    return dg.MaterializeResult(
        metadata={
            "Number of rows uploaded": row_count,
        },
    )


@dg.asset(group_name="sftp")
def customer() -> dg.MaterializeResult:
    """Load customer data and upload to SFTP server."""
    row_count = upload_data(CUSTOMER_FILE_NAME)
    return dg.MaterializeResult(
        metadata={
            "Number of rows uploaded": row_count,
        },
    )


@dg.asset(group_name="sftp")
def product() -> dg.MaterializeResult:
    """Load product data and upload to SFTP server."""
    row_count = upload_data(PRODUCT_FILE_NAME)
    return dg.MaterializeResult(
        metadata={
            "Number of rows uploaded": row_count,
        },
    )
