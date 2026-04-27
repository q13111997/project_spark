from psycopg2.extras import execute_values
import psycopg2


def dim_product_writer(batch_df, postgres_conf):

    sql = """
        INSERT INTO dim_product (
            product_id,
            name,
            product_type,
            sku,
            price,
            min_price,
            max_price,
            qty,
            collection_id,
            collection,
            category,
            category_name
        )
        VALUES %s
        ON CONFLICT (product_id) DO UPDATE
        SET
            name = EXCLUDED.name,
            product_type = EXCLUDED.product_type,
            sku = EXCLUDED.sku,
            price = EXCLUDED.price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            qty = EXCLUDED.qty,
            collection_id = EXCLUDED.collection_id,
            collection = EXCLUDED.collection,
            category = EXCLUDED.category,
            category_name = EXCLUDED.category_name
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
            batch.append((
                row.product_id,
                row.name,
                row.product_type,
                row.sku,
                row.price,
                row.min_price,
                row.max_price,
                row.qty,
                row.collection_id,
                row.collection,
                row.category,
                row.category_name
            ))

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