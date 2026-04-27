def main():
    import os
    from pyspark.sql import SparkSession

    import util.config as conf
    from util.logger import Log4j

    from transform.parse_json import parse_json
    from transform.parse_dim_store import parse_dim_store
    from transform.parse_dim_agent import parse_dim_agent
    from transform.parse_dim_ip_location import parse_dim_ip_location
    from transform.parse_dim_product import parse_dim_product

    from sink.dim_store_writer import dim_store_writer
    from sink.dim_agent_writer import dim_agent_writer
    from sink.dim_ip_location_writer import dim_ip_location_writer
    from sink.dim_product_writer import dim_product_writer

    bin_file = "IP-COUNTRY-REGION-CITY.BIN"

    # ========================
    # Load config
    # ========================
    base_dir = os.path.dirname(os.path.abspath(__file__))
    conf_path = os.path.join(base_dir, "spark.conf")

    cfg = conf.Config(conf_path)

    spark_conf = cfg.spark_conf
    kafka_conf = cfg.kafka_conf
    postgres_conf = {
        "host": "postgres",
        "database": "postgres",
        "user": "postgres",
        "password": "UnigapPostgres@123"
    }

    # ========================
    # Init Spark
    # ========================
    spark = (
        SparkSession.builder
        .config(conf=spark_conf)
        .getOrCreate()
    )

    # distribute file tới executors
    spark.sparkContext.addFile("hdfs:///app/data/IP-COUNTRY-REGION-CITY.BIN")

    log = Log4j(spark)

    log.info("Start loading product CSV")

    # ========================
    # Read CSV từ HDFS
    # ========================
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("hdfs:///app/data/products_info.csv")
    )

    if df.rdd.isEmpty():
        log.info("Empty CSV, skip")
        return

    # ========================
    # Transform
    # ========================
    df_product = parse_dim_product(df)

    # control parallelism = số connection DB
    df_product = df_product.repartition(4)

    # ========================
    # Write
    # ========================
    dim_product_writer(df_product, postgres_conf)

    log.info("Finished loading dim_product")

    # ========================
    # Read Kafka
    # ========================
    df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_conf)
        .load()
    )

    df_parsed = parse_json(df)

    # ========================
    # Batch processing
    # ========================
    def process_batch(batch_df, batch_id):
        log.info(f"Processing batch_id={batch_id}")

        if batch_df.rdd.isEmpty():
            return

        df_product = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("hdfs:///app/data/products_info.csv")
        )

        # parse
        df_store = parse_dim_store(batch_df)
        df_agent = parse_dim_agent(batch_df)
        df_ip = parse_dim_ip_location(batch_df)
        df_product = parse_dim_product(df_product)

        # (optional) control số partition = số connection
        df_store = df_store.repartition(4)
        df_agent = df_agent.repartition(4)
        df_ip = df_ip.repartition(4)
        df_product = df_product.repartition(4)

        # write riêng từng dimension
        dim_store_writer(df_store, postgres_conf)
        dim_agent_writer(df_agent, postgres_conf)
        dim_ip_location_writer(df_ip, postgres_conf, "IP-COUNTRY-REGION-CITY.BIN")
        dim_product_writer(df_product, postgres_conf)

    # ========================
    # Streaming
    # ========================
    query = (
        df_parsed.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        #.option("checkpointLocation", "/tmp/checkpoint/dim_pipeline")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()