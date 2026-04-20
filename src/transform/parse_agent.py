from user_agents import parse
from pyspark.sql.functions import col, concat, lit

def parse_user_agent(ua_string):
    if ua_string is None:
        return (None, None)
    
    ua = parse(ua_string)
    
    return (ua.os.family, ua.browser.family)

def parse_dim_user_agent(df):
    df_user_agent = df \
        .select("user_agent") \
        .filter(col("user_agent").isNotNull()) \
        .withColumn("store_name", concat(lit("Store "), col("store_id"))) \
        .dropDuplicates(["store_id"])
    
    return df_user_agent