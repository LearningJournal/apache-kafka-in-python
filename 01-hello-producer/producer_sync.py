from confluent_kafka import Producer
import socket

if __name__ == '__main__':
    print("Starting Kafka Producer")

    producer_config = {'client.id': socket.gethostname(),
                       'bootstrap.servers': 'localhost:9092'}

    print("Creating Producer")
    producer = Producer(producer_config)

    print("Producing Kafka Message")
    for i in range(1, 101):
        for j in range(1, 10001):
            producer.produce('hello-producer', key=str(j*i), value="Simple Message-" + str(j*i))
            producer.flush()

    print("Finished Kafka Producer")
