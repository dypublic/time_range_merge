import random
import uuid
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import window
import os
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.7"


'''
PYTHONUNBUFFERED=1;SPARK_HOME=D:\project\python\tracing_time_merge\spark-2.4.5-bin-hadoop2.7;JAVA_HOME=C:\java-se-8u41-ri;HADOOP_HOME=D:\project\python\tracing_time_merge\hadoop;Path=C:\Program Files (x86)\NetSarang\Xshell 6\\;C:\Windows\system32\;C:\Windows\;C:\Windows\System32\Wbem\;C:\Windows\System32\WindowsPowerShell\v1.0\\;C:\Windows\System32\OpenSSH\\;C:\Program Files\Git\cmd\;C:\Program Files\Calibre2\\;C:\Go\bin\;C:\Users\yue.dai\AppData\Local\Programs\Python\Python37\Scripts\\;C:\Users\yue.dai\AppData\Local\Programs\Python\Python37\\;C:\Users\yue.dai\AppData\Local\Microsoft\WindowsApps\;C:\Users\yue.dai\go\bin\;D:\project\python\tracing_time_merge\hadoop\bin
'''
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
    start_log = {"root_id":root_uuid, "span_id": span_id, "start": start}
    end_log = {"root_id":root_uuid, "span_id": span_id, "end": end}
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
    df.show(truncate=False)

#     content = df.selectExpr("CAST(value AS STRING)")
#
# #perf_hilton-adapter_raw:1:110729716: key=None value=b'{"app_name": "hilton-adapter", "level": "INFO", "logger": "PERF_LOGGER", "ip": "172.31.40.7", "host": "hilton-adapter", "thread": "logExecutor-3", "message": "<echo.token=c52436f3b46e4acbbb65e372c186b7eb> <res.id.derby=DK1221055BHXPW> <channel=BOOKINGCOM> <supplier=HILTON> <process=QueryRes> <process.result=Success> <process.duration=1> <process.supplier=None> <hotel.supplier=UNKNOWN> <force.sell=false>", "type": "perf", "timestamp": "2020-01-22T10:56:14.133"}'
#
#     schema = StructType() \
#         .add("ip", StringType()) \
#         .add("app_name", StringType()) \
#         .add("message", StringType()) \
#         .add("timestamp", TimestampType())
#
#     # 通过from_json，定义schema来解析json
#     res = content.select(from_json("value", schema).alias("data")).select("data.*")
#     windowedCounts = res.withWatermark("timestamp", "1 minutes").groupBy(
#         window("timestamp", "1 minutes", "10 seconds"),
#         "ip", "app_name"
#     ).count()
#
#
#     query = windowedCounts.writeStream \
#         .format("console").option("truncate", "false") \
#         .outputMode("append") \
#         .start()
#
#     query.awaitTermination()