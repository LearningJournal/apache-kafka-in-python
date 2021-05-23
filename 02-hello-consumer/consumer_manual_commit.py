from confluent_kafka import Consumer, KafkaException

if __name__ == '__main__':
    print("Starting Kafka Consumer")
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'learning_journal',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    print("Creating Consumer")
    consumer = Consumer(consumer_config)

    print("Subscribe to Topic")
    consumer.subscribe(['hello-consumer'])

    try:
        print("Consuming Kafka Message")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                print('Topic:%s Partition:[%d] at offset %d with key %s' % (
                    msg.topic(), msg.partition(), msg.offset(), msg.key()))
                print('Value:%s' % (msg.value()) + '\n')
                consumer.commit(asynchronous=True)

    except KeyboardInterrupt:
        print(' Aborted by user\n')
    finally:
        consumer.close()
