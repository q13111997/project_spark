import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit
from pyspark.sql.types import *

import util.config as conf
from util.logger import Log4j

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

    # 2. Parse Kafka JSON
    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    df_store = df_parsed \
        .select("store_id") \
        .filter(col("store_id").isNotNull()) \
        .withColumn("store_name", concat(lit("Store "), col("store_id")))
    
    query = df_store.writeStream \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()

main()