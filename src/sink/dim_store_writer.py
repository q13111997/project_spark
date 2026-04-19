import psycopg2
from psycopg2.extras import execute_values

def upsert_dim_store(batch_df):
    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="UnigapPostgres@123"
    )
    cursor = conn.cursor()

    batch = []
    batch_size = 5000

    for row in batch_df:
        batch.append((row.store_id, row.store_name))
        if len(batch) >= batch_size:
            execute_values(cursor, """
                INSERT INTO dim_store (store_id, store_name)
                VALUES %s
                ON CONFLICT (store_id) DO UPDATE SET store_name = EXCLUDED.store_name
            """, batch)
            batch.clear()
    if batch:
        execute_values(cursor, """
            INSERT INTO dim_store (store_id, store_name)
            VALUES %s
            ON CONFLICT (store_id) DO UPDATE SET store_name = EXCLUDED.store_name
        """, batch)
    conn.commit()
    cursor.close()
    conn.close()

def process_batch_dim_store(batch_df, batch_id):
    batch_df = batch_df.dropDuplicates(["store_id"])
    batch_df.foreachPartition(upsert_dim_store)