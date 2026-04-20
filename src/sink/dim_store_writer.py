from psycopg2.extras import execute_values
import psycopg2


def dim_store_writer(batch_df, postgres_conf):
    sql = """
        INSERT INTO dim_store (store_id, store_name)
        VALUES %s
        ON CONFLICT (store_id) DO UPDATE
        SET store_name = EXCLUDED.store_name
    """

    def write_partition(rows):
        conn = psycopg2.connect(
            host=postgres_conf["host"],
            database=postgres_conf["database"],
            user=postgres_conf["user"],
            password=postgres_conf["password"]
        )
        cursor = conn.cursor()

        batch = []
        batch_size = 5000

        for row in rows:
            batch.append((row.store_id, row.store_name))

            if len(batch) >= batch_size:
                execute_values(cursor, sql, batch)
                conn.commit()
                batch.clear()

        if batch:
            execute_values(cursor, sql, batch)
            conn.commit()

        cursor.close()
        conn.close()

    batch_df.foreachPartition(write_partition)