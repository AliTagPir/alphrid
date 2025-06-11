from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

class CheckMissingChartsOperator(BaseOperator):

    def __init__(self, postgres_conn_id="external_alphrid_db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        year = yesterday.year

        expected_daily = {
            f"daily_{d.strftime('%Y-%m-%d')}"
            for d in pd.date_range(yesterday - timedelta(days=13), yesterday)
        }

        current_iso = yesterday.isocalendar()
        current_week = current_iso.week
        expected_weekly = {
            f"weekly_{year}-W{week:02d}" for week in range(1, current_week + 1)
        }

        expected_monthly = {
            f"monthly_{year}-{month:02d}" for month in range(1, yesterday.month + 1)
        }

        # Query existing chart keys from chart_cache using PostgresHook
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = """
            SELECT chart_key FROM chart_cache
            WHERE chart_key LIKE 'daily_%'
            OR chart_key LIKE 'weekly_%'
            OR chart_key LIKE 'monthly_%'
        """
        rows = hook.get_records(sql)
        existing_keys = {row[0] for row in rows}

        # Identify missing chart keys
        missing_daily = sorted(expected_daily - existing_keys)
        missing_weekly = sorted(expected_weekly - existing_keys)
        missing_monthly = sorted(expected_monthly - existing_keys)

        self.log.info(f"Missing daily charts: {missing_daily}")
        self.log.info(f"Missing weekly charts: {missing_weekly}")
        self.log.info(f"Missing monthly charts: {missing_monthly}")

        return {
            "daily": missing_daily,
            "weekly": missing_weekly,
            "monthly": missing_monthly
        }