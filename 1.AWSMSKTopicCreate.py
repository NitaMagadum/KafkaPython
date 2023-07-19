from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer

#AWS MSK cluster configuration
bootstrap_server = "b-3.mskcluster.nr5art.c8.kafka.us-east-1.amazonaws.com:9098"
topic_name = "awskafkatopic1"
replication_factor = 3 

def create_topic():
    # Connect to the Kafka cluster
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_server)

    # NewTopic object with the desired topic configuration
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=replication_factor)

    # create the topic
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")

    # Close the admin client
    admin_client.close()

if __name__ == "__main__":
    create_topic()
