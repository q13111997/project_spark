import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"base_dir: {base_dir}")

    conf_path_file = base_dir + "/spark.conf"

    conf = conf.Config(conf_path_file)

    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    df.printSchema()

    query = df.select(col("value").cast(StringType()).alias("value")) \
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
