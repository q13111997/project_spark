from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

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