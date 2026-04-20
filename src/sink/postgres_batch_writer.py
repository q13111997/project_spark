import psycopg2
from psycopg2.extras import execute_values

def postgres_batch_writer(batch_df, sql_query, batch_size=5000):
    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="UnigapPostgres@123"
    )
    cursor = conn.cursor()

    batch = []

    for row in batch_df:
        batch.append((row.store_id, row.store_name))
        if len(batch) >= batch_size:
            execute_values(cursor, sql_query, batch)
            conn.commit()
            batch.clear()
    if batch:
        execute_values(cursor, sql_query, batch)
        conn.commit()
    cursor.close()
    conn.close()