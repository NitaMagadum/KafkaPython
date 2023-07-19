from json import dumps
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['b-3.mskcluster.nr5art.c8.kafka.us-east-1.amazonaws.com:9098'],key_serializer=str.encode,value_serializer=lambda x: dumps(x).encode('utf-8'))
topic_name='hello_world3'
data1 = {'number' : 1}
data2 = {'number' : 2}
data3 = {'number' : 3}
data4 = {'number' : 4}
data5 = {'number' : 5}
data6 = {'number' : 6}
producer.send(topic_name,  key='ping',value=data1)
producer.send(topic_name, key='ping',value=data2)
producer.send(topic_name, key='ping',value=data3)
producer.send(topic_name, key='pong',value=data4)
producer.send(topic_name, key='pong',value=data5)
producer.send(topic_name, key='pong',value=data6)
producer.close()