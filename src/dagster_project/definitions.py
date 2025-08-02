import dagster as dg

from dagster_project.defs.jobs import daily_upload_job
from dagster_project.defs.schedules import daily_upload_schedule

defs = dg.Definitions(
    assets=dg.load_assets_from_package_name("dagster_project.defs.assets"),
    jobs=[daily_upload_job],
    schedules=[daily_upload_schedule],
)
