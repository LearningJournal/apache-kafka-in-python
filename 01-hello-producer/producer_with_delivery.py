from confluent_kafka import Producer, Message
import socket

delivered_records = 0


def callback(err, msg):
    global delivered_records
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {}, partition [{}], and @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':
    print("Starting Kafka Producer")

    producer_config = {'client.id': socket.gethostname(),
                       'bootstrap.servers': 'localhost:9092'}

    print("Creating Producer")
    producer = Producer(producer_config)

    print("Producing Kafka Message")
    for i in range(1, 101):
        for j in range(1, 11):
            producer.produce('hello-producer', key=str(j * i), value="Simple Message-" + str(j * i),
                             on_delivery=callback)
        producer.poll()

    producer.flush()
    print("{} messages were delivered to topic {}!".format(delivered_records, 'hello-producer'))
    print("Finished Kafka Producer")
