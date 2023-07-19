from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import datetime
from decimal import *
import threading
from time import sleep
from uuid import uuid4,UUID

#"for failure and success  of message delivery"
def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for message record{}:{}".format(msg.key(),err))
        return
    else:
        print("User record{} Successfully produced to {}[{}] at offset".format(msg.key(),msg.topic(),msg.partition(),msg.offset()))

#"Define kafka configiration dictionary"
kafka_configuration ={
    'bootstrap.servers':'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'UIJCXU6VBEMWHNEQ',
    'sasl.password': 'KVFU+Dx9OGi29RR7LTYTvtYIIp0MDz2TgQdz6tulym8Fnle79iJa7XRwpnOtuBM2'
}

#Define schema registry client dictionary
schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-e0919.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('FCAGJF3LMXUFR5TA', 'qkrapb5iyVDai5jINv+j7WWJX+zRh0NPa6nEg3J4mBY7wHzprXp2cxCM9aP/HXfT')
})

#Fetch the latest avro schema 
subject_name='retail_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
#print(schema_str)

## Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer =StringSerializer('utf_8')
avro_serializer=AvroSerializer(schema_registry_client,schema_str)

#Define a serializing producer
producer =SerializingProducer({
    'bootstrap.servers': kafka_configuration['bootstrap.servers'],
    'security.protocol': kafka_configuration['security.protocol'],
    'sasl.mechanisms': kafka_configuration['sasl.mechanisms'],
    'sasl.username': kafka_configuration['sasl.username'],
    'sasl.password': kafka_configuration['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

# read data into a pandas dataframe
retail_df= pd.read_csv('retail_data.csv')
retail_df=retail_df.fillna('null')

#Iterate over dataframe and produce to kafka

for index,row in retail_df.iterrows():
    #create a disctionary for row
    value=row.to_dict()
    producer.produce(topic='retail_data',key=str(index),value=value,on_delivery=delivery_report)
    producer.flush()
    #break

print("Data successfully published to Kafka")
        

