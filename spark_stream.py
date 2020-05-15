# !/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import window

# sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 spark_stream.py 
brokers = "172.27.0.236:9092, 172.27.0.75:9092"
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4") \
        .appName("pyspark_structured_streaming_kafka") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", "perf_hilton-adapter_raw") \
        .load()

    content = df.selectExpr("CAST(value AS STRING)")

#perf_hilton-adapter_raw:1:110729716: key=None value=b'{"app_name": "hilton-adapter", "level": "INFO", "logger": "PERF_LOGGER", "ip": "172.31.40.7", "host": "hilton-adapter", "thread": "logExecutor-3", "message": "<echo.token=c52436f3b46e4acbbb65e372c186b7eb> <res.id.derby=DK1221055BHXPW> <channel=BOOKINGCOM> <supplier=HILTON> <process=QueryRes> <process.result=Success> <process.duration=1> <process.supplier=None> <hotel.supplier=UNKNOWN> <force.sell=false>", "type": "perf", "timestamp": "2020-01-22T10:56:14.133"}'

    schema = StructType() \
        .add("ip", StringType()) \
        .add("app_name", StringType()) \
        .add("message", StringType()) \
        .add("timestamp", TimestampType())

    # 通过from_json，定义schema来解析json
    res = content.select(from_json("value", schema).alias("data")).select("data.*")
    windowedCounts = res.withWatermark("timestamp", "1 minutes").groupBy(
        window(res.timestamp, "1 minutes", "10 seconds"),
        "ip", "app_name"
    ).count()


    query = windowedCounts.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
