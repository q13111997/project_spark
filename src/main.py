import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit, schema_of_json
from pyspark.sql.types import *
from transform.parse_json import parse_json
from transform.parse_dim_store import parse_dim_store
from sink.dim_store_writer import upsert_dim_store, process_batch_dim_store

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

    df_parsed = parse_json(df)

    df_store = parse_dim_store(df_parsed)
       
    query = df_store.writeStream \
        .foreachBatch(process_batch_dim_store) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    query.awaitTermination()

main()