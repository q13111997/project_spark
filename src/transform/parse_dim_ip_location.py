from pyspark.sql.functions import col

def parse_dim_ip_location(df):
    return (
        df
        .select(col("ip"))
        .where(col("ip").isNotNull())
        .distinct()
    )