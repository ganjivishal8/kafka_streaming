import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import chain

#GCP VARIABLES
GCP_PROJECT_ID = "data-project-406509"
dataset_name = "vishal_db"
TEMPORARY_BUCKET_NAME = "vish_bucket1"
table_name="kafka-streaming"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'


#Initializing the Spark Session

def initialize_spark(appname,packages):
    
    spark = SparkSession.builder. \
                appName(appname). \
                config("spark.jars.packages", packages). \
                getOrCreate()
    
    
    # logging.info("spark created +++++")
    return spark
    
#Reading the data from kafka topic

def get_streaming_data(spark,server,topic):

    kafka_df = spark.readStream. \
            format("kafka"). \
            option("kafka.bootstrap.servers", server). \
            option("subscribe", topic). \
            option("startingOffsets", "latest"). \
            load().\
            withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")). \
            withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))
    # logging.info("Data Streaming started ++++++")
    return kafka_df
    
# Parsing String data to Dataframe Schema
def parse_data(df):
    uber_schema=(StructType()
                .add("VendorID", IntegerType(),True)
                .add("tpep_pickup_datetime", TimestampType(),True)
                .add("tpep_dropoff_datetime", TimestampType(),True)
                .add("passenger_count", IntegerType(),True)
                .add("trip_distance",FloatType(),True)
                .add("pickup_longitude", FloatType(), True)
                .add("pickup_latitude", FloatType(),True)
                .add("RatecodeID",IntegerType(),True)
                .add("store_and_fwd_flag", StringType(), True)
                .add("dropoff_longitude", FloatType(), True)
                .add("dropoff_latitude",FloatType(),True)
                .add("payment_type", IntegerType(),True)
                .add("fare_amount", FloatType(),True)
                .add("extra", FloatType(), True)
                .add("mta_tax", FloatType(), True)
                .add("tip_amount", FloatType(),True)
                .add("tolls_amount", FloatType(),True)
                .add("improvement_surcharge", FloatType(), True)
                .add("total_amount", FloatType(), True)
    )

    trans_df = df.withColumn("value_ar", from_json(df["value"], uber_schema)).select("value_ar.*")

    return trans_df

#Configuring and Writing to Bigquery
def foreach_batch_function(dataframe,id):
    dataframe.write.format('bigquery') \
            .option("table", f"{GCP_PROJECT_ID}.{dataset_name}.{table_name}") \
            .option('parentProject', GCP_PROJECT_ID) \
            .option("temporaryGcsBucket", TEMPORARY_BUCKET_NAME) \
            .mode("append") \
            .save()
    
def stream_to_bq(df,checkpointLocation):
    df \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation",checkpointLocation)\
    .foreachBatch(foreach_batch_function)\
    .start() \
    .awaitTermination()

#Mapping Dictionary
def map_values(dict):
    expr=create_map([lit(x) for x in chain(*dict.items())])
    return expr


def main():

    # GLOBAL VARIABLES
    #Big Query Connector Jar file
    #Spark SQl Kafka jar
    scala_version = "2.12"
    spark_version = "3.5.0"
    packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'

    checkpointLocation="my_check"

    payment_type={1: "Credit card",2:"Cash",3:"No charge",4:"Dispute",5:"Unknown",6:"Voided tip"}

    rate_code={1:"Standard rate",2:"JFK",3:"Newark",4:"Nassau or Westchester",5:"Negotiated fare",6:"Group ride"}

    spark=initialize_spark("Kafka-Streaming",packages)  #1
    
    if spark:
        data=get_streaming_data(spark,"localhost:9092","kafkademo1") #2


        if data:
            trans_df=parse_data(data) #3

            #total_travel_time and speed
            trans_df=trans_df.withColumn("total_travel_time",round((col('tpep_dropoff_datetime').cast('long')-col('tpep_pickup_datetime').cast('long'))/(60*60),2)).\
                            withColumn("speed",round(col('trip_distance')/col('total_travel_time'),2))
            
            payment_expr=map_values(payment_type)
            rate_code_expr=map_values(rate_code)
            
            #Mapping Payment type and Ratecode Name
            trans_df=trans_df.withColumn("payment_name", payment_expr[col("payment_type")])\
                .withColumn("rateCodeName",rate_code_expr[col("RatecodeID")])
            
            #Dropping Null Values
            trans_df=trans_df.na.drop()

            stream_to_bq(trans_df,checkpointLocation) #4


if __name__ == '__main__':
    main()





