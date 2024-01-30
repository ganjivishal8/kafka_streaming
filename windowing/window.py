import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


scala_version = "2.12"
spark_version = "3.5.0"

packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
spark = SparkSession.builder. \
                    master("local[3]").\
                    appName("Event-time Windowing"). \
                    config("spark.jars.packages", packages). \
                    config("spark.sql.shuffle.partitions",2).\
                    getOrCreate()


stock_schema= StructType([
    StructField('CreatedTime', StringType(),True),\
    StructField('Type', StringType(),True),\
    StructField('Amount', IntegerType(),True),\
    StructField('BrokerCode',StringType(),True)
])

BOOT_STRAP = "localhost:9092"
TOPIC = "window"

kafka_df = spark.readStream. \
                format("kafka"). \
                option("kafka.bootstrap.servers", BOOT_STRAP). \
                option("subscribe", TOPIC). \
                option("startingOffsets", "earliest"). \
                load()
                

                # withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")). \
                # withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))

# value_df=kafka_df.withColumn("value",from_json(kafka_df["value"],stock_schema)).select("value.*")

value_df=kafka_df.select(from_json(col("value").cast("string"),stock_schema).alias("value"))

trade_df=value_df.select("value.*").\
            withColumn("CreatedTime", to_timestamp(col("CreatedTime"),"yyyy-MM-dd HH:mm:ss")).\
            withColumn("Buy",expr("case when Type == 'BUY' then Amount else 0 end")).\
            withColumn("Sell",expr("case when Type == 'SELL' then Amount else 0 end"))


# Tumbling Window

window_df=trade_df.groupBy(window(col("CreatedTime"),"15 minute")).\
                agg(sum("Buy").alias("TotalBuy"),sum("Sell").alias("TotalSell"))





##Window with Watermark

# window_df=trade_df.withWatermark("CreatedTime","30 minute").\
#             groupBy(window(col("CreatedTime"),"15 minute")).\
#                 agg(sum("Buy").alias("TotalBuy"),sum("Sell").alias("TotalSell"))


out_df=window_df.select("window.start","window.end","TotalBuy","TotalSell")


query = out_df. \
        writeStream. \
        format("console"). \
        outputMode("update"). \
        trigger(processingTime="1 minute").\
        start(). \
        awaitTermination()




# query = trade_df. \
#         writeStream. \
#         format("console"). \
#         outputMode("update"). \
#         start(). \
#         awaitTermination()