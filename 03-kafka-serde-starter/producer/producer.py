from utils.config import KAFKA_CONFIGS
from utils.config import LOG


def delivery_report(err, msg):
    if err is not None:
        LOG.error("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    LOG.info("Record {} successfully produced to {} [{}] at offset {}".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    LOG.info("Producer Configs {}".format(KAFKA_CONFIGS['producer']))

    # Step 1: Create producer configuration
    #   - Create Serializer instance for JSON/AVRO/PROTOBUF types
    #   - Add key.serializer & value.serializer apart from bootstrap.servers

    # Step 2: Create SerializingProducer instance

    # Step 3: Write code to produce data
    #   - Use poll and flush properly
    pass


if __name__ == '__main__':
    main()
