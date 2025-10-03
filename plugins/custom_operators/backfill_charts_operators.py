from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import re

from utils.chart_util_lib import (
    generate_daily_chart_for,
    generate_weekly_chart_for,
    generate_monthly_chart_for,
    generate_yearly_chart_for,
)

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
        
        self.log.info(f"Missing daily: {sorted(missing_daily)}")
        self.log.info(f"Missing weekly: {sorted(missing_weekly)}")
        self.log.info(f"Missing monthly: {sorted(missing_monthly)}")

        # Sort final lists for XCom
        return {
            "daily": sorted(missing_daily),
            "weekly": sorted(missing_weekly),
            "monthly": sorted(missing_monthly),
        }

class GenerateMissingChartsOperator(BaseOperator):
    def __init__(self, chart_key, postgres_conn_id="external_alphrid_db", **kwargs):
        super().__init__(**kwargs)
        self.chart_key = chart_key
        self.postgres_conn_id = postgres_conn_id
    
    def execute(self, context):
        self.log.info(f"Generating chart for key: {self.chart_key}")
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()

        try:
            if self.chart_key.startswith("daily_"):
                date_str = self.chart_key.replace("daily","")
                date_obj = datetime.strptime(date_str, "")
                result = generate_daily_chart_for(date_obj, conn)
            
            elif self.chart_key.startswith("weekly_"):
                match = re.match(r"weekly_(\d{4})-W(\d{2})", self.chart_key)
                if not match:
                    raise ValueError(f"Invalid weekly chart_key: {self.chart_key}")
                year, week = map(int, match.group())
                start_of_week = datetime.strptime(f"{year}-W{week}-1", "%G-W%V-%u").date()
                result = generate_weekly_chart_for(start_of_week, conn)

            elif self.chart_key.startswith("monthly_"):
                match = re.match(r"monthly_(\d{4})-(\d{2})", self.chart_key)
                if not match:
                    raise ValueError(f"Invalid monthly chart_key: {self.chart_key}")
                year, month = map(int, match.groups())
                result = generate_monthly_chart_for(year, month, conn)

            elif self.chart_key.stratswith("yearly_"):
                year = int(self.chart_key.replace("yearly_",""))
                result = generate_yearly_chart_for(year, conn)
            
            else:
                raise ValueError(f"Unrecognised chart_key format: {self.chart_key}")
            
            self.log.info(result)
            return result
            
        except Exception as e:
            self.log.error(f"Failed to generate chart for {self.chart_key}: {e}")
            raise

        finally:
            conn.close()
            self.log.info("Database connection closed")
