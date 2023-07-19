import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config={
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'UIJCXU6VBEMWHNEQ',
    'sasl.password': 'KVFU+Dx9OGi29RR7LTYTvtYIIp0MDz2TgQdz6tulym8Fnle79iJa7XRwpnOtuBM2',
    'group.id': 'group11',
    'auto.offset.reset': 'earliest'
}

schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-e0919.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('FCAGJF3LMXUFR5TA', 'qkrapb5iyVDai5jINv+j7WWJX+zRh0NPa6nEg3J4mBY7wHzprXp2cxCM9aP/HXfT')
})

# get latest schema from schema registry
subject_name ='retail_data-value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str

#Create Avro deserializer for key and value
key_deserializer = StringDeserializer('utf_8')
value_deserializer = AvroDeserializer(schema_registry_client,schema_str)

#Define a deerializing consumer
consumer=DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': value_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

#subscribe to reati_data topic
consumer.subscribe(['retail_data'])

try:
    while True:
        message =consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        print('Successfully consumed record with key {} and value {}'.format(message.key(),message.value()))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()





