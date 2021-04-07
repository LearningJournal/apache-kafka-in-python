#!/usr/bin/env python
# =============================================================================
#
# Producer 2 - Asynchronous writes, with notification of delivery success or failure
#
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

import logging
import socket
import sys

from confluent_kafka import Producer

LOG = logging.getLogger('learning_journal')
LOG.setLevel(logging.DEBUG)
BASE_FMT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S')

console_out = logging.StreamHandler(sys.stdout)
console_out.setFormatter(BASE_FMT)
LOG.addHandler(console_out)
delivered_records = 0


def acked(err, msg1):
    global delivered_records
    if err is not None:
        LOG.error("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        LOG.info("Produced record to topic {} partition [{}] @ offset {}"
                 .format(msg1.topic(), msg1.partition(), msg1.offset()))


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
                             on_delivery=acked)
        producer.poll()

    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, 'hello-producer'))
    print("Finished Kafka Producer")
