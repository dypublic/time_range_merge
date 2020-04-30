import random
import uuid
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import window
from pyspark.sql.functions import mean, sum, col, when, struct, greatest, least
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import max as spark_max

import os

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.7"

# PYTHONUNBUFFERED=1;SPARK_HOME=D:\project\python\tracing_time_merge\spark-2.4.5-bin-hadoop2.7;JAVA_HOME=C:\java-se-8u41-ri;HADOOP_HOME=D:\project\python\tracing_time_merge\hadoop;Path=C:\Program Files (x86)\NetSarang\Xshell 6\\;C:\Windows\system32\;C:\Windows\;C:\Windows\System32\Wbem\;C:\Windows\System32\WindowsPowerShell\v1.0\\;C:\Windows\System32\OpenSSH\\;C:\Program Files\Git\cmd\;C:\Program Files\Calibre2\\;C:\Go\bin\;C:\Users\yue.dai\AppData\Local\Programs\Python\Python37\Scripts\\;C:\Users\yue.dai\AppData\Local\Programs\Python\Python37\\;C:\Users\yue.dai\AppData\Local\Microsoft\WindowsApps\;C:\Users\yue.dai\go\bin\;D:\project\python\tracing_time_merge\hadoop\bin

'''
SPARK_LOCAL_IP=0.0.0.0
SPARK_EXECUTOR_MEMORY=500M
SPARK_MASTER_HOST=0.0.0.0
SPARK_WORKER_MEMORY=1G
PYSPARK_PYTHON=python3
'''


def generate_time_range_pairs():
    for i in range(10):
        start = random.uniform(0, 30)
        duration = random.uniform(0, 10)
        end = start + duration
    return start, end


def generate_span_log(root_uuid):
    start, end = generate_time_range_pairs()
    span_id = str(uuid.uuid4())
    logs = []
    start_log = {"root_id": root_uuid, "span_id": span_id, "start": start}
    end_log = {"root_id": root_uuid, "span_id": span_id, "end": end}
    logs.append(json.dumps(start_log))
    logs.append(json.dumps(end_log))
    return logs


def generate_tracing_log(nodes_num):
    root = str(uuid.uuid4())
    logs = []
    for i in range(nodes_num):
        span_logs = generate_span_log(root)
        logs.extend(span_logs)
    return logs


def generate_batch_tracing_log(batch):
    logs = []
    for i in range(batch):
        t_logs = generate_tracing_log(3)
        logs.extend(t_logs)
    return logs


s = generate_batch_tracing_log(10)
print(s)


def merge_time(exist_list, pair):
    if isinstance(exist_list, tuple):
        return [exist_list]
    new_list = []

    for exist_pair in exist_list:
        if pair[0] > exist_pair[1] or pair[1] < exist_pair[0]:
            new_list.append(exist_pair)
            print("skip:", pair, exist_pair)
            continue
        print("merging:", pair, exist_pair)
        pair = (min(pair[0], exist_pair[0]), max(pair[1], exist_pair[1]))
        print("merged:", pair)

    new_list.append(pair)
    return new_list

# brokers = "172.27.0.236:9092, 172.27.0.75:9092"
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("spark://192.168.56.101:7077") \
        .appName("pyspark_tracing_log_caculate_remote") \
        .config("spark.executor.memory", "1GB") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "2") \
        .config("spark.local.ip", "192.168.56.1") \
        .config("spark.driver.host", "192.168.56.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    json_logs = generate_batch_tracing_log(10)
    # ds = spark.createDataFrame()
    sc = spark.sparkContext
    logsRDD = sc.parallelize(json_logs)
    df = spark.read.json(logsRDD)
    expr = [spark_min(col("start")).alias("start"), spark_max(col("end")).alias("end")]
    df = df.select("root_id", "span_id", "start", "end").groupBy("root_id", "span_id").agg(*expr)
    #
    # df = df.alias("t1").join(df.alias("t2")).where(col("t1.root_id") == col("t2.root_id"))
    # df = df.select(col("t1.root_id").alias("t1_root_id"), col("t1.start"), col("t1.end"), col("t2.start"), col("t2.end"))
    #
    # df = df.withColumn("expand",
    #                    when(((col("t1.start") > col("t2.end")) | (col("t1.end") < col("t2.start"))),
    #                         struct(col("t1.start").alias("t_start"), col("t1.end").alias("t_end"))) \
    #                    .otherwise(
    #                        struct(least(col("t1.start"), col("t2.start")).alias("t_start"), greatest(col("t1.end"), col("t2.end")).alias("t_end"))
    #                    )
    #                    # 1
    #                    )
    # df = df.dropDuplicates(["t1_root_id", "expand"])
    rdd = df.rdd.map(lambda x: (x["root_id"], (x["start"], x["end"])))
    rdd = rdd.reduceByKey(merge_time)
    # df = df.orderBy("t1_root_id")
    print(rdd.take(10))
    # df.show(truncate=False)
