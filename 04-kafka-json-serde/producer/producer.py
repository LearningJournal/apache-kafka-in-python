from uuid import uuid4

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

from model.user import User
from utils.config import KAFKA_CONFIGS
from utils.config import LOG


def user_to_dict(user, ctx):
    return dict(first_name=user.first_name,
                last_name=user.last_name,
                email=user.email,
                age=user.age)


def delivery_report(err, msg):
    if err is not None:
        LOG.error("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    LOG.info("User record {} successfully produced to {} [{}] at offset {}".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    with open("../resources/schema/user.json") as fp:
        schema_str = fp.read()
    schema_registry_conf = {'url': KAFKA_CONFIGS['producer']['schema-registry']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    json_serializer = JSONSerializer(schema_str=schema_str, schema_registry_client=schema_registry_client,
                                     to_dict=user_to_dict)

    producer_conf = {'bootstrap.servers': KAFKA_CONFIGS['producer']['bootstrap-servers'],
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)

    topic = KAFKA_CONFIGS['producer']['topic']
    LOG.info("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        try:
            user_first_name = input("Enter first name: ")
            user_last_name = input("Enter last name: ")
            user_email = input("Enter email: ")
            user_age = int(input("Enter age: "))
            user = User(first_name=user_first_name,
                        last_name=user_last_name,
                        email=user_email,
                        age=user_age)
            producer.produce(topic=topic, key=str(uuid4()), value=user,
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            LOG.error("Invalid input, discarding record...")
            continue
        producer.poll(0.0)

    LOG.info("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    main()
