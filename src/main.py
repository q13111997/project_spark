import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit, schema_of_json
from pyspark.sql.types import *

import util.config as conf
from util.logger import Log4j

def parse_json(df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("time_stamp", TimestampType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("local_time", TimestampType(), True),
        StructField("show_recommendation", BooleanType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("option", StringType(), True)
    ])

    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")
    
    return df_parsed

def parse_dim_store(df):
    df_store = df \
        .select("store_id") \
        .filter(col("store_id").isNotNull()) \
        .withColumn("store_name", concat(lit("Store "), col("store_id")))
    
    return df_store

def upsert_dim_store(batch_df):
    import psycopg2
    from psycopg2 import execute_values
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="UnigapPostgres@123"
    )
    cursor = conn.cursor()

    batch = []
    batch_size = 1000

    for row in batch_df:
        batch.append(row.store_id, row.store_name)
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

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(base_dir)

    conf_path_file = base_dir + "/spark.conf"

    cfg = conf.Config(conf_path_file)

    spark_conf = cfg.spark_conf
    kafka_conf = cfg.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()
    
    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kafka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    df.printSchema()

    df_parsed = parse_json(df)

    df_store = parse_dim_store(df_parsed)
       
    query = df_store.writeStream \
        .foreachBatch(process_batch_dim_store) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    query.awaitTermination()

main()