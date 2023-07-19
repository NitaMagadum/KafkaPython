from time import sleep
from json import dumps
from kafka import KafkaProducer


#Lab 1: Write message to a partition (mentioning the partition number while publishing the message)

topic_name='awskafkatopic1'
producer = KafkaProducer(bootstrap_servers=['lb-3.mskcluster.nr5art.c8.kafka.us-east-1.amazonaws.com:9098'],value_serializer=lambda x: dumps(x).encode('utf-8'))
data1 = {'number' : 1}
data2 = {'number' : 2}
data3 = {'number' : 3}
data4 = {'number' : 4}
data5 = {'number' : 5}
data6 = {'number' : 6}
producer.send(topic_name, value=data1,partition=1)
producer.send(topic_name, value=data2,partition=1)
producer.send(topic_name, value=data3,partition=1)
producer.send(topic_name, value=data4,partition=2)
producer.send(topic_name, value=data5,partition=2)
producer.send(topic_name, value=data6,partition=0)
producer.close()