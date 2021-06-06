confluent local services start

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka-json-serde

kafka-json-schema-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 \
--property print.key=true --property key.separator=":" \
--key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
--value-deserializer io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer \
--topic kafka-json-serde --from-beginning

confluent local destroy