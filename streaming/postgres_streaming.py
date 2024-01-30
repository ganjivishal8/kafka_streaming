import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

findspark.init()

url="jdbc:postgresql://localhost:5432/scd_2"
username='postgres'
password='Vish@08'


scala_version = "2.12"
spark_version = "3.5.0"

packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
spark = SparkSession.builder. \
                    appName("KafkaSparkConnect"). \
                    config("spark.jars.packages", packages). \
                    getOrCreate()

BOOT_STRAP = "localhost:9092"
TOPIC = "kafkademo1"

kafka_df = spark.readStream. \
                format("kafka"). \
                option("kafka.bootstrap.servers", BOOT_STRAP). \
                option("subscribe", TOPIC). \
                option("startingOffsets", "latest"). \
                load().\
                withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")). \
                withColumn("value", regexp_replace(col("value"), "^\"|\"$", "")) \

# base_df = kafka_df. \
#         selectExpr("CAST(value AS STRING) AS value")

# base_df = kafka_df. \
        # selectExpr("value")


sample_schema=StructType([StructField('id', StringType(), True), \
                          StructField('first_name',StringType(),True),\
                          StructField('last_name',StringType(),True),\
                          StructField('company',StringType(),True),\
                          StructField('role',StringType(),True)
]                         
)


# sample_schema = (
#         StructType()
#         .add("id", IntegerType())
#         .add("first_name", StringType())
#         .add("last_name", StringType())
#         .add("company", StringType())
#         .add("role", StringType())
#     )

fin_df = kafka_df.withColumn("value", from_json(kafka_df["value"], sample_schema)).select("value.*") 

# fin_df = base_df.select(
#         from_json(col("value"), sample_schema).alias("sample")
#     ).select("sample.*")

# query = fin_df. \
#         writeStream. \
#         format("console"). \
#         outputMode("append"). \
#         start(). \
#         awaitTermination()


def foreach_batch_function(df,id):
    df.write.mode("append") \
      .format("jdbc") \
      .option("driver","org.postgresql.Driver") \
      .option("url", url) \
      .option("dbtable", "kafka_stream") \
      .option("user", username) \
      .option("password", password) \
      .save()
    pass
# 
fin_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function)\
    .start() \
    .awaitTermination()