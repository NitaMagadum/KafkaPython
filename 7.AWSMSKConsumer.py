from kafka import KafkaConsumer

consumer =KafkaConsumer('awskafkatopic1',bootstrap_servers=['b-3.mskcluster.nr5art.c8.kafka.us-east-1.amazonaws.com:9098'])

for msg in consumer:
    rec_data =msg.value.decode('utf-8')
    r= rec_data.replace('"','')
    record=r.strip('\\n')
    print(record)