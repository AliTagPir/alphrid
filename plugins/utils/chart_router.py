from datetime import datetime
import re

from utils.chart_util_lib import (
    generate_daily_chart_for,
    generate_weekly_chart_for,
    generate_monthly_chart_for,
    generate_yearly_chart_for,
)

def run_chart_generation_by_key(chart_key: str, conn) -> str:
    """Routes a chart_key to the appropriate chart generation function."""
    try:
        if chart_key.startswith("daily_"):
            date_str = chart_key.replace("daily_", "")
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            return generate_daily_chart_for(date_obj, conn)

        elif chart_key.startswith("weekly_"):
            match = re.match(r"weekly_(\d{4})-W(\d{2})", chart_key)
            if not match:
                raise ValueError(f"Invalid weekly chart_key: {chart_key}")
            year, week = map(int, match.groups())
            start_of_week = datetime.strptime(f"{year}-W{week}-1", "%G-W%V-%u").date()
            return generate_weekly_chart_for(start_of_week, conn)

        elif chart_key.startswith("monthly_"):
            match = re.match(r"monthly_(\d{4})-(\d{2})", chart_key)
            if not match:
                raise ValueError(f"Invalid monthly chart_key: {chart_key}")
            year, month = map(int, match.groups())
            return generate_monthly_chart_for(year, month, conn)

        elif chart_key.startswith("yearly_"):
            year = int(chart_key.replace("yearly_", ""))
            return generate_yearly_chart_for(year, conn)

        else:
            raise ValueError(f"Unrecognized chart_key format: {chart_key}")

    except Exception as e:
        raise RuntimeError(f"[ERROR] Chart generation failed for {chart_key}: {e}")