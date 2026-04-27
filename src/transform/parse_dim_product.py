from pyspark.sql.functions import col


def parse_dim_product(df):
    return (
        df.select(
            col("product_id").cast("long"),
            col("name"),
            col("product_type"),
            col("sku"),
            col("price").cast("double"),
            col("min_price").cast("double"),
            col("max_price").cast("double"),
            col("qty").cast("int"),
            col("collection_id").cast("int"),
            col("collection"),
            col("category").cast("int"),
            col("category_name")
        )
    )