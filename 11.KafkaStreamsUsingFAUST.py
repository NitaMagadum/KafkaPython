import faust

app=faust.App('demo-Stream',broker='b-3.mskcluster.nr5art.c8.kafka.us-east-1.amazonaws.com:9098')

topic = app.topic('awskafkatopic1', value_type=str,value_serializer='raw')

@app.agent(topic)
async def processor(stream):
    async for message in stream:
        print(f'Received {message}')


#faust -A KafkaStreamUsingFAUST worker -l info