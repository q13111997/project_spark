from sink.postgres_batch_writer import postgres_batch_writer

def dim_store_writer(batch_df, batch_id):
    dim_store_sql = """
        INSERT INTO dim_store (store_id, store_name)
        VALUES %s
        ON CONFLICT (store_id) DO UPDATE
        SET store_name = EXCLUDED.store_name
    """
    def _write_partition(batch_df):
        postgres_batch_writer(batch_df, dim_store_sql)
    batch_df.foreachPartition(_write_partition)