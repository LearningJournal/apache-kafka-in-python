from utils.config import KAFKA_CONFIGS
from utils.config import LOG


def main():
    LOG.info("Consumer Configs {}".format(KAFKA_CONFIGS['consumer']))
    # Step 1: Create consumer configuration
    #   - Create Deserializer instance for JSON/AVRO/PROTOBUF types
    #   - Add key.deserializer & value.deserializer apart from bootstrap.servers, group.id, auto.offset.reset

    # Step 2: Create DeserializingConsumer instance

    # Step 3: Subscribe to topic and Write code to consume & transform data
    #   - Use poll & close properly
    pass


if __name__ == '__main__':
    main()
