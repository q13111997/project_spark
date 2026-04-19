from pyspark.sql.functions import col, concat, lit

def parse_dim_store(df):
    df_store = df \
        .select("store_id") \
        .filter(col("store_id").isNotNull()) \
        .withColumn("store_name", concat(lit("Store "), col("store_id")))
    
    return df_store