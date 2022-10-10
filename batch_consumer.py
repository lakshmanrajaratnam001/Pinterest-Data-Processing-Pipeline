from kafka import KafkaConsumer
from json import loads
from json import dumps
import json
import boto3
from numpy import insert
from sqlalchemy import create_engine

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

batch_consumer.subscribe(topics='MyFirstKafkaTopic')

s3 = boto3.client('s3')
counter = 0

for message in batch_consumer:

    with open("raw_data.json", "w") as f:
        json.dump(message.value, f)
    file_name = str(counter) + '.json'
    s3.upload_file('/Users/lakshmanrajaratnam/Desktop/pintrest/Pinterest_App/raw_data.json', 'pintrestbucket', file_name)
    counter += 1
    print('success')
   

