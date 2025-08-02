import dagster as dg

date_format = "%Y-%m-%d%z"


daily_partition = dg.DailyPartitionsDefinition(
    start_date="2025-06-25", timezone="Australia/Sydney"
)
