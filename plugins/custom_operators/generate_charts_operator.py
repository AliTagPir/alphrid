import os
import pandas as pd
import json
import plotly.express as px
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

class GenerateChartsOperator(BaseOperator):
    
    def __init__(self, pg_conn_id, output_dir, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_conn_id = pg_conn_id
        self.output_dir = output_dir

    def generate_daily_chart(self, conn):
        today = datetime.now().date()
        today_str = today.strftime("%Y-%m-%d")

        # Query toady's data
        query = f"""
            SELECT * FROM master_tracker_data
            WHERE date = '{today_str}'
        """
        df = pd.read_sql(query, conn)

        if df.empty:
            self.log.info(f"No data found for {today_str}. Skipping daily chart generation")
            return
        
        # Melt DataFrame into long format
        df_melted = df.melt(
            id_vars=["date"],
            value_vars=["working", "programming", "exercise", "leisure"],
            var_name="Activity",
            value_name="Hours"
        )

        # Generate Pie Chart with Plotly
        fig = px.pie(
            df_melted,
            names="Activity",
            values="Hours",
            title=f"Activity Breakdown for {today_str}"
        )

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save chart as PNG
        png_path = os.path.join(self.output_dir, f"daily_{today_str}.png")
        fig.write_image(png_path)
        self.log.info(f"Saved daily chart PNG: {png_path}")

        # Generate and store interactive HTML
        chart_html = fig.to_html(full_html=False, include_plotyjs='cdn')

        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO chart_cache (chart_key, chart_type, chart_html)
            VALUES (%s, %s, %s)
            ON CONFLICT (chart_key) DO UPDATE
            SET chart_html = EXCLUDED.chart_html,
                last_updated = NOW();
        """
        chart_key = f"daily_{today_str}"
        cursor.execute(insert_sql, (chart_key, "pie", chart_html))
        conn.commit()
        cursor.close()
        
        self.log.info(f"Sorted daily chart HTML in chart_cache with key: {chart_key}")

    def generate_weekly_chart(self, conn):
        # Calculate the start of the current week (Monday)
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        start_of_week_str = start_of_week.strftime("%Y-%m-%d")
        week_key = start_of_week.strftime("weekly_%Y-W%U")  # E.g., 'weekly_2025-W15'

        # Query data for the current week
        query = f"""
            SELECT * FROM master_tracker_data
            WHERE date >= '{start_of_week_str}' AND date <= '{today}'
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            self.log.info(f"No data found for the week starting {start_of_week_str}. Skipping weekly chart generation.")
            return
        
        # Melt DataFrame to long format
        df_melted = df.melt(
            id_vars=["date"],
            value_vars=["working", "programming", "exercise", "leisure"],
            value_name="Activity",
            value_name="Hours"
        )

        # Generate Grouped Bar Chart
        fig = px.bar(
            df_melted,
            x="date",
            y="Hours",
            color="Activity",
            barmode="group",
            title=f"Activity Breakdown for Week Starting {start_of_week_str}"
        )

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save chart as PNG
        png_path = os.path.join(self.output_dir, f"weekly_chart_{week_key}.png")
        fig.write_image(png_path)
        self.log.info(f"Saved weekly chart PNG: {png_path}")

        # Generate and store interactive HTML
        chart_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO chart_cache (chart_key, chart_type, chart_html)
            VALUES (%s, %s, %s)
            ON CONFLICT (chart_key) DO UPDATE
            SET chart_html = EXCLUDED.chart_html,
                last_updated = NOW();
        """
        cursor.execute(insert_sql, (week_key, "bar", chart_html))
        conn.commit()
        cursor.close()

        self.log.info(f"Stored weekly chart HTML in chart_cache with key: {week_key}")
    
    def generate_monthly_chart(self, conn):
        # Determine start of current month
        today = datetime.now().date()
        start_of_month = today.replace(day=1)
        start_of_month_str = start_of_month.strftime("%Y-%m-%d")
        month_key = today.strftime("monthly_%Y-%m")  # e.g., 'monthly_2025-04'

        # Query data for current month
        query = f"""
            SELECT * FROM master_tracker_data
            WHERE date >= '{start_of_month_str}' AND date <= '{today}'
        """
        df = pd.read_sql(query, conn)

        if df.empty:
            self.log.info(f"No data found for {month_key}. Skipping monthly chart generation.")
            return

        # Melt to long format
        df_melted = df.melt(
            id_vars=["date"],
            value_vars=["working", "programming", "exercise", "leisure"],
            var_name="Activity",
            value_name="Hours"
        )

        # Generate line chart
        fig = px.line(
            df_melted,
            x="date",
            y="Hours",
            color="Activity",
            markers=True,
            title=f"Daily Activity Trends for {month_key}"
        )

        # Ensure output dir exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save PNG
        png_path = os.path.join(self.output_dir, f"monthly_chart_{month_key}.png")
        fig.write_image(png_path)
        self.log.info(f"Saved monthly chart PNG: {png_path}")

        # Save HTML version
        chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO chart_cache (chart_key, chart_type, chart_html)
            VALUES (%s, %s, %s)
            ON CONFLICT (chart_key) DO UPDATE
            SET chart_html = EXCLUDED.chart_html,
                last_updated = NOW();
        """
        cursor.execute(insert_sql, (month_key, "line", chart_html))
        conn.commit()
        cursor.close()

        self.log.info(f"Stored monthly chart HTML in chart_cache with key: {month_key}")

    def generate_yearly_chart(self, conn):
        # Calculate first day of the year
        today = datetime.now().date()
        start_of_year = today.replace(month=1, day=1)
        start_of_year_str = start_of_year.strftime("%Y-%m-%d")
        year_key = today.strftime("yearly_%Y")  # e.g., 'yearly_2025'

         # Query all data for the year
        query = f"""
            SELECT * FROM master_tracker_data
            WHERE date >= '{start_of_year_str}' AND date <= '{today}'
        """
        df = pd.read_sql(query, conn)

        if df.empty:
            self.log.info(f"No data found for {year_key}. Skipping yearly chart generation.")
            return
        
         # Add a 'month' column (e.g., 'Jan', 'Feb', 'Mar'...)
        df['month'] = pd.to_datetime(df['date']).dt.strftime("%b")

        # Aggregate total hours per activity per month
        df_grouped = df.groupby('month')[["working", "programming", "exercise", "leisure"]].sum().reset_index()

        # Melt to long format
        df_melted = df_grouped.melt(
            id_vars=["month"],
            var_name="Activity",
            value_name="Hours"
        )

        # Generate line chart
        fig = px.line(
            df_melted,
            x="month",
            y="Hours",
            color="Activity",
            markers=True,
            title=f"Monthly Activity Trends for {year_key}"
        )
        fig.update_layout(xaxis_type='category')
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save PNG
        png_path = os.path.join(self.output_dir, f"yearly_chart_{year_key}.png")
        fig.write_image(png_path)
        self.log.info(f"Saved yearly chart PNG: {png_path}")

        # Save HTML version
        chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO chart_cache (chart_key, chart_type, chart_html)
            VALUES (%s, %s, %s)
            ON CONFLICT (chart_key) DO UPDATE
            SET chart_html = EXCLUDED.chart_html,
                last_updated = NOW();
        """
        cursor.execute(insert_sql, (year_key, "line", chart_html))
        conn.commit()
        cursor.close()

        self.log.info(f"Stored yearly chart HTML in chart_cache with key: {year_key}")

    def execute(self,context):
        self.log.info("Starting GenerateChartsOperator...")

        # Step 1: Connect to the external Postgres DB
        hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = hook.get_conn()

        try:
            # Step 2: Generate each chart
            self.generate_daily_chart(conn)
            self.generate_weekly_chart(conn)
            self.generate_monthly_chart(conn)
            self.generate_yearly_chart(conn)

            self.log.info("All charts generated successfully.")

        except Exception as e:
            self.log.error(f"Error during chart generation: {e}")
            raise

        finally:
            conn.close()
            self.log.info("Database connection closed.")
    