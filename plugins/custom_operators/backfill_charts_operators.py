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

        # Expected daily, Weekly and Monthly chart keys
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

        # Identify missing keys
        missing_daily = expected_daily - existing_keys
        missing_weekly = expected_weekly - existing_keys
        missing_monthly = expected_monthly - existing_keys

        # Cascade logic
        cascade_weekly = set()
        cascade_monthly = set()

        # From missing daily -> week + month
        for key in missing_daily:
            date_str = key.replace("daily_", "")
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            iso_week = date_obj.isocalendar()
            week_key = f"weekly_{iso_week[0]}-W{iso_week[1]:02d}"
            month_key = f"monthly_{date_obj.year}-{date_obj.month:02d}"
            cascade_weekly.add(week_key)
            cascade_monthly.add(month_key)
        
        # From missing weekly -> month
        for key in missing_weekly:
            parts = key.replace("weekly_", "").split("-W")
            w_year, w_week = int(parts[0]), int(parts[1])
            # Approximate ISO week to the monday of that week
            date_obj = datetime.strptime(f"{w_year}-W{w_week}-1", "%G-W%V-%u").date()
            month_key = f"monthly_{date_obj.year}-{date_obj.month:02d}"
            cascade_monthly.add(month_key)
        
        # Merge cascade into missing sets
        missing_weekly |= cascade_weekly
        missing_monthly |= cascade_monthly
        
        # Sort final lists for XCom
        return {
            "daily": sorted(missing_daily),
            "weekly": sorted(missing_weekly),
            "monthly": sorted(missing_monthly),
        }

