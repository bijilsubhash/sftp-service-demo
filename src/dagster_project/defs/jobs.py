import dagster as dg
from dagster_project.defs.partitions import daily_partition

daily_upload_job = dg.define_asset_job(
    name="daily_upload_job",
    selection=dg.AssetSelection.all(),
    partitions_def=daily_partition,
    description="Upload daily data to SFTP server",
)
