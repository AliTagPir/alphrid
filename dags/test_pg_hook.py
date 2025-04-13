from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="external_alphrid_db")
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT 1;")
print("Connection successful:", cursor.fetchone())