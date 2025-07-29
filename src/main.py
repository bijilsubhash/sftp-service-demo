import sys
import re

from datetime import datetime
from pathlib import Path


from common.utils.logging_util import Logger
from sftp.services.faker_service import FakerService
from sftp.services.sftp_service import SFTPClient
from sftp.models.config import SFTPConfig

import dagster as dg

logger = Logger(__name__)

sftp_client = SFTPClient(
    hostName=dg.EnvVar("hostName").get_value() or SFTPConfig().hostName,
    port=dg.EnvVar("port").get_value() or SFTPConfig().port,
    userName=dg.EnvVar("userName").get_value() or SFTPConfig().userName,
    password=dg.EnvVar("password").get_value() or SFTPConfig().password,
)


def upload_data(
    csv_file: str, data_dir: Path | None = None, date: str | None = None
) -> int:
    row_count = 0
    if date:
        date_formatted = datetime.strptime(date, "%d-%m-%Y").strftime("%Y%m%d")
        remote_path = f"input/{date_formatted}/{csv_file}"
    else:
        remote_path = f"input/{csv_file}"

    try:
        sftp_conn = sftp_client.connect()

        if csv_file in ["customers.csv", "products.csv"]:
            local_file = Path("data") / csv_file
        elif data_dir:
            local_file = data_dir / csv_file

        if local_file.exists():
            sftp_client.put(sftp_conn, str(local_file), remote_path)
            row_count = len(local_file.read_text().splitlines())
            logger.info(f"Uploaded {csv_file} from {local_file}")
        else:
            logger.warning(f"File not found: {local_file}")
    finally:
        sftp_client.close()

    return row_count


# used for local testing
if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        raise ValueError(
            "No argument provided. Please provide a date in format DD-MM-YYYY"
        )

    if len(args) != 1:
        raise ValueError("Only one argument is allowed")

    if not re.match(r"^\d{2}-\d{2}-\d{4}$", args[0]):
        raise ValueError("Invalid date format")

    faker_service = FakerService(args[0])
    faker_service.generate_data(size=1000)

    upload_data("orders.csv", faker_service.output_dir, args[0])
