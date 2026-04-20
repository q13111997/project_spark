
## Chạy chương trình

```shell
docker run --rm -ti --name project_spark \
--network=streaming-network \
-v ./:/spark \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
-e KAFKA_BOOTSTRAP_SERVERS='46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294' \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="UnigapKafka@2024";' \
unigap/spark:3.5 bash -c "(cd /spark/src && zip -r /tmp/src.zip .) &&
conda env create --file /spark/env/spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/src.zip \
--deploy-mode client \
--master yarn \
/spark/src/main.py"
```