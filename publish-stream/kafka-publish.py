from kafka import KafkaProducer
import logging
import json
import csv
logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda K:json.dumps(K).encode('utf-8'))

def transform_data(line: json):

    return {
        'VendorID': int(line["VendorID"]), 
        'tpep_pickup_datetime': line["tpep_pickup_datetime"], 
        'tpep_dropoff_datetime': line["tpep_dropoff_datetime"], 
        'passenger_count': int(line["passenger_count"]), 
        'trip_distance': float(line["trip_distance"]), 
        'pickup_longitude': float(line["pickup_longitude"]), 
        'pickup_latitude': float(line["pickup_latitude"]), 
        'RatecodeID': int(line["RatecodeID"]), 
        'store_and_fwd_flag': line["store_and_fwd_flag"], 
        'dropoff_longitude': float(line["dropoff_longitude"]), 
        'dropoff_latitude': float(line["dropoff_latitude"]), 
        'payment_type': int(line["payment_type"]), 
        'fare_amount': float(line["fare_amount"]), 
        'extra': float(line["extra"]), 
        'mta_tax': float(line["mta_tax"]), 
        'tip_amount': float(line["tip_amount"]), 
        'tolls_amount': float(line["tolls_amount"]), 
        'improvement_surcharge': float(line["improvement_surcharge"]), 
        'total_amount': float(line["total_amount"])
    }

with open('C:/Users/SpringMl/kafka/uber_data_1.csv', 'r') as file:
    # reader = csv.DictReader(file, delimiter = '\t')
    reader = csv.DictReader(file)
    
    for messages in reader:
        messages=transform_data(messages)
        print(messages)
        producer.send('kafkademo1', messages)
        producer.flush()


# print(messages)
        # d = yaml.safe_load(messages)
        # jd = dumps(messages)