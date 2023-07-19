from kafka import kafkaProducer
import json
from time import sleep

prod =kafkaProducer(bootstrap_server=['b-3.mskcluster.nr5art.c8.kafka.us-east-1.amazonaws.com:9098'])

f= open("/home/ec2-user/Data.csv","r")

for msg in f:
    data = msg
    prod.send('awskafkatopic1',json.dumps(data).encode('utf-8'))
    sleep(2)

#prod.flush()

#