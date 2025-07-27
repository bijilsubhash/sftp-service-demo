import dagster as dg
from datetime import datetime, timedelta

date_format = "%Y-%m-%d%z"


daily_partition = dg.DailyPartitionsDefinition(
    start_date=(datetime.now() - timedelta(days=1)).strftime(date_format)
)
