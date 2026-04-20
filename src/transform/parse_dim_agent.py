from user_agents import parse
from pyspark.sql.functions import udf, col, sha2, concat_ws
from pyspark.sql.types import *

schema = StructType([
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True)
])

def parse_ua(ua):
    if ua is None:
        return (None, None)
    
    ua_parsed = parse(ua)
    os = ua_parsed.os.family
    browser = ua_parsed.browser.family
    
    return (os, browser)

parse_ua_udf = udf(parse_ua, schema)

def parse_dim_agent(df):
    df_user_agent = df \
        .select("user_agent") \
        .filter(col("user_agent").isNotNull()) \
        .withColumn("parsed", parse_ua_udf(col("user_agent"))) \
        .select(
            col("user_agent"),
            col("parsed.os").alias("os"),
            col("parsed.browser").alias("browser")
        ) \
        .dropDuplicates(["user_agent"]) \
        .withColumn(
            "user_agent_id",
            sha2(concat_ws("||", col("os"), col("browser")), 256)
        )
    
    return df_user_agent