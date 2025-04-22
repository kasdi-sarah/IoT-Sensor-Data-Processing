from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import sys
import os
import threading
import time
# Kafka配置
KAFKA_BROKER = '<kafka-broker>'
TOPIC = 'sensor'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: Process_iot_streaming.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Process_iot_streaming") \
    .getOrCreate()

# 定义传感器数据的Schema
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("rainfall",DoubleType(), True),
    StructField("wind_speed",DoubleType(), True),
    StructField("UV_index",DoubleType(), True)
])
raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"LIQN5IFO4VF2U5KV\" password=\"Qp6Zl3PuSayJSCRD8/pXJBJNNPIE6NCKWIMUsUk701c2HX6Pf/zVFg2Z1kVJzJei\";") \
        .option("subscribe", topics) \
        .option("startingOffsets", "earliest") \
        .load()

# 解析Kafka消息
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
parsed_stream.writeStream \
        .format("memory") \
        .queryName("raw_stream") \
        .outputMode("append") \
        .trigger(processingTime="2 seconds") \
        .start()
#Créer une table temporaire pour afficher le contenu du stream
def query_tables():
        while True:
            print("==== Sensor Stream ====")
            spark.sql("SELECT * FROM raw_stream").show(truncate=False)
            
            
            time.sleep(2)

#Utiliser des threads pour interroger périodiquement les tables de mémoire
thread = threading.Thread(target=query_tables, daemon=True)
thread.start()
#  Provoquer l'alarme
# Création des alertes avec conditions multiples
alerts = parsed_stream.filter(
    (col("temperature") > 35.0) | 
    (col("humidity") < 20.0) | 
    (col("UV_index") > 8.0) | 
    (col("rainfall") > 50.0) | 
    (col("wind_speed") > 20.0)
).withColumn(
    "alert",
    when(col("temperature") > 35.0, "High Temperature")
    .when(col("humidity") < 20.0, "Low Humidity")
    .when(col("UV_index") > 8.0, "High UV Index")
    .when(col("rainfall") > 50.0, "Heavy Rainfall")
    .when(col("wind_speed") > 20.0, "High Wind Speed")
    .otherwise("No Alert")
).select(
    "sensor_id", 
    "temperature", 
    "humidity", 
    "timestamp", 
    "rainfall", 
    "wind_speed", 
    "UV_index", 
    "alert"
)
#  output
query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="30 second")\
    .start()

query.awaitTermination()