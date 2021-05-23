from confluent_kafka import Consumer, KafkaException


def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        for a_partition in partitions:
            print("Topic {} [{}] committed upto offset {}".format(
                a_partition.topic, a_partition.partition, a_partition.offset))


if __name__ == '__main__':
    print("Starting Kafka Consumer")
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'learning_journal_git',
        'auto.offset.reset': 'earliest',
        'on_commit': commit_completed
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

    except KeyboardInterrupt:
        print(' Aborted by user\n')
    finally:
        consumer.close()
