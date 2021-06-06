from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

from model.user import User
from utils.config import KAFKA_CONFIGS
from utils.config import LOG


def dict_to_user(obj, ctx):
    if obj is None:
        return None

    return User(first_name=obj['first_name'],
                last_name=obj['last_name'],
                email=obj['email'],
                age=obj['age'])


def main():
    with open("../resources/schema/user.json") as fp:
        schema_str = fp.read()
    json_deserializer = JSONDeserializer(schema_str=schema_str,
                                         from_dict=dict_to_user)

    consumer_conf = {'bootstrap.servers': KAFKA_CONFIGS['consumer']['bootstrap-servers'],
                     'key.deserializer': StringDeserializer('utf_8'),
                     'value.deserializer': json_deserializer,
                     'group.id': KAFKA_CONFIGS['consumer']['group-id'],
                     'auto.offset.reset': KAFKA_CONFIGS['consumer']['auto-offset']}

    consumer = DeserializingConsumer(consumer_conf)

    consumer.subscribe([KAFKA_CONFIGS['consumer']['topic']])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = msg.value()
            if user is not None:
                profile_description = "My name is {}. I am {} years old. Contact me at {}".format(
                    user.first_name + " " + user.last_name, user.age, user.email)
                LOG.info("\nUser record: {}\nprofile_description: {}".format(msg.key(), profile_description))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    main()
