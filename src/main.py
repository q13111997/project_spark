def main():
    import os
    from pyspark.sql import SparkSession

    import util.config as conf
    from util.logger import Log4j

    from transform.parse_json import parse_json
    from transform.parse_dim_store import parse_dim_store
    from transform.parse_dim_agent import parse_dim_agent

    from sink.dim_store_writer import dim_store_writer
    from sink.dim_agent_writer import dim_agent_writer

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

    log = Log4j(spark)

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

        df_store = parse_dim_store(batch_df)
        df_agent = parse_dim_agent(batch_df)

        dim_store_writer(df_store, postgres_conf)
        dim_agent_writer(df_agent, postgres_conf)

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