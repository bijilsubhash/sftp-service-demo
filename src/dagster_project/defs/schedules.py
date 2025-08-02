import dagster as dg
from dagster_project.defs.jobs import daily_upload_job

daily_upload_schedule = dg.build_schedule_from_partitioned_job(
    job=daily_upload_job,
    cron_schedule="0 1 * * *",
    execution_timezone="Australia/Sydney",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
