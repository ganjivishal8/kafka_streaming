
# #Sliding Window Example Data

# {"CreatedTime":"2023-10-05 09:54:00","Reading":36.2}
# {"CreatedTime":"2023-10-05 09:59:00","Reading":36.5}
# {"CreatedTime":"2023-10-05 10:04:00","Reading":36.8}
# {"CreatedTime":"2023-10-05 10:09:00","Reading":36.2}
# {"CreatedTime":"2023-10-05 10:14:00","Reading":36.5}
# {"CreatedTime":"2023-10-05 10:19:00","Reading":36.3}
# {"CreatedTime":"2023-10-05 10:24:00","Reading":37.7}
# {"CreatedTime":"2023-10-05 10:29:00","Reading":37.2}

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


scala_version="2.12"
spark_version="3.5.0"

packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
spark = SparkSession.builder. \
                    master("local[3]").\
                    appName("Sliding Windowing"). \
                    config("spark.jars.packages", packages). \
                    config("spark.sql.shuffle.partitions",2).\
                    getOrCreate()

sensor_schema=StructType([
    StructField('CreatedTime',StringType(),True),\
    StructField('Reading',DoubleType(),True)
])

BOOT_STRAP = "localhost:9092"
TOPIC = "swindow"

kafka_df = spark.readStream. \
                format("kafka"). \
                option("kafka.bootstrap.servers", BOOT_STRAP). \
                option("subscribe", TOPIC). \
                option("startingOffsets", "earliest"). \
                load()

value_df=kafka_df.select(from_json(col("value").cast("string"),sensor_schema).alias("value"))

stage_df=value_df.select("value.*").\
            withColumn("CreatedTime", to_timestamp(col("CreatedTime"),"yyyy-MM-dd HH:mm:ss"))

window_df=stage_df.withWatermark("CreatedTime","30 minute").\
            groupBy(window(col("CreatedTime"),"15 minute","5 minute")).\
                agg(max("Reading").alias("Max_reading"))

out_df=window_df.select("window.start","window.end","Max_reading")

# query = value_df. \
#         writeStream. \
#         format("console"). \
#         outputMode("update"). \
#         start(). \
#         awaitTermination()


query = out_df. \
        writeStream. \
        format("console"). \
        outputMode("update"). \
        trigger(processingTime="2 minute").\
        start(). \
        awaitTermination()