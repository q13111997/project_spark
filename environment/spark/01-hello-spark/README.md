Tại thư mục `spark`, chạy lệnh sau:

```shell
docker container stop hello-spark || true && 
docker container rm hello-spark || true && 
docker run --rm -ti --name hello-spark \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--deploy-mode client \
--master yarn \
/spark/01-hello-spark/hello_spark.py"
```