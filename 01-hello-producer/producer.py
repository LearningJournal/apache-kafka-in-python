from confluent_kafka import Producer

if __name__ == '__main__':
    print("Starting Kafka Producer")

    topic = 'hello-producer'
    producer_config = {'bootstrap.servers': 'localhost:9092'}

    print("Creating Producer")
    producer = Producer(producer_config)

    print("Producing Kafka Message")
    for i in range(100000):
        producer.produce(topic, key=str(i), value="Simple Message-" + str(i))

    producer.flush()
    print("Finished Kafka Producer")
