import os
import findspark
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"]="C:\Program Files\Java\jdk-17"

findspark.init

BOOT_STRAP = "localhost:9092"
TOPIC = "kafkademo"

scala_version = "2.12"
spark_version = "3.5.0"

packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'


spark = SparkSession.builder. \
                    appName("KafkaSparkConnect"). \
                    config("spark.jars.packages", packages). \
                    getOrCreate()


kafka_df = spark.readStream. \
                format("kafka"). \
                option("kafka.bootstrap.servers", BOOT_STRAP). \
                option("subscribe", TOPIC). \
                load()

query = kafka_df. \
        selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)"). \
        writeStream. \
        format("console"). \
        outputMode("append"). \
        start()

query.awaitTermination()
