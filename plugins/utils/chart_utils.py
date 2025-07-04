from datetime import datetime, timedelta, date
import pandas as pd
import plotly.express as px
from calendar import month_abbr
import re

def generate_daily_chart_for(date_obj: date, conn):
    date_str = date_obj.strftime("%Y-%m-%d")
    chart_key = f"daily_{date_str}"
    
    query = f"""
        SELECT * FROM master_tracker_data
        WHERE date = '{date_str}'
    """
    df = pd.read_sql(query, conn)
    
    if df.empty:
        return f"[SKIPPED] No data for {chart_key}"

    df_melted = df.melt(
        id_vars=["date"],
        value_vars=["working", "programming", "exercise", "leisure"],
        var_name="Activity",
        value_name="Hours"
    )

    fig = px.pie(df_melted, names="Activity", values="Hours",
                 title=f"Daily Productivity Breakdown for {date_str}")
    chart_json = fig.to_json()

    _store_chart(chart_key, "pie", chart_json, conn)
    return f"[SUCCESS] Daily chart generated: {chart_key}"


def generate_weekly_chart_for(start_of_week: date, conn):
    end_of_week = start_of_week + timedelta(days=6)
    start_str = start_of_week.strftime("%Y-%m-%d")
    end_str = end_of_week.strftime("%Y-%m-%d")
    week_key = f"weekly_{start_of_week.isocalendar()[0]}-W{start_of_week.isocalendar()[1]:02d}"

    query = f"""
        SELECT * FROM master_tracker_data
        WHERE date BETWEEN '{start_str}' AND '{end_str}'
    """
    df = pd.read_sql(query, conn)
    
    if df.empty:
        return f"[SKIPPED] No data for {week_key}"

    df_melted = df.melt(
        id_vars=["date"],
        value_vars=["working", "programming", "exercise", "leisure"],
        var_name="Activity",
        value_name="Hours"
    )

    fig = px.bar(df_melted, x="date", y="Hours", color="Activity", barmode="group",
                 title=f"Weekly Productivity Breakdown ({start_str} to {end_str})")
    chart_json = fig.to_json()

    _store_chart(week_key, "bar", chart_json, conn)
    return f"[SUCCESS] Weekly chart generated: {week_key}"


def generate_monthly_chart_for(year: int, month: int, conn):
    start_date = date(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month.replace(day=1) - timedelta(days=1)

    month_key = f"monthly_{year}-{month:02d}"
    query = f"""
        SELECT * FROM master_tracker_data
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql(query, conn)

    if df.empty:
        return f"[SKIPPED] No data for {month_key}"

    df_melted = df.melt(
        id_vars=["date"],
        value_vars=["working", "programming", "exercise", "leisure"],
        var_name="Activity",
        value_name="Hours"
    )

    fig = px.line(df_melted, x="date", y="Hours", color="Activity", markers=True,
                  title=f"Daily Trends for {month_key}")
    chart_json = fig.to_json()

    _store_chart(month_key, "line", chart_json, conn)
    return f"[SUCCESS] Monthly chart generated: {month_key}"


def generate_yearly_chart_for(year: int, conn):
    start_date = date(year, 1, 1)
    end_date = datetime.today().date().replace(day=1) - timedelta(days=1)

    year_key = f"yearly_{year}"
    query = f"""
        SELECT * FROM master_tracker_data
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql(query, conn)

    if df.empty:
        return f"[SKIPPED] No data for {year_key}"

    month_order = list(month_abbr)[1:end_date.month + 1]
    df['month'] = pd.to_datetime(df['date']).dt.strftime("%b")
    df['month'] = pd.Categorical(df['month'], categories=month_order, ordered=True)

    df_grouped = df.groupby('month')[["working", "programming", "exercise", "leisure"]].sum()
    df_grouped = df_grouped.reindex(month_order, fill_value=0).reset_index()

    df_melted = df_grouped.melt(id_vars=["month"], var_name="Activity", value_name="Hours")

    fig = px.line(df_melted, x="month", y="Hours", color="Activity", markers=True,
                  title=f"Monthly Activity Trends for {year_key}")
    fig.update_layout(xaxis_type='category')
    chart_json = fig.to_json()

    _store_chart(year_key, "line", chart_json, conn)
    return f"[SUCCESS] Yearly chart generated: {year_key}"


def _store_chart(chart_key, chart_type, chart_json, conn):
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO chart_cache (chart_key, chart_type, chart_json)
        VALUES (%s, %s, %s)
        ON CONFLICT (chart_key) DO UPDATE
        SET 
            chart_type = EXCLUDED.chart_type,
            chart_json = EXCLUDED.chart_json,
            last_updated = NOW();
    """
    cursor.execute(insert_sql, (chart_key, chart_type, chart_json))
    conn.commit()
    cursor.close()

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