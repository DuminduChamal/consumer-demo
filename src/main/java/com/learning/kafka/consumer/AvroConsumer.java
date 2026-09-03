package com.learning.kafka.consumer;

import com.learning.kafka.OrderEventAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// Change vs. JsonConsumer: KafkaAvroDeserializer instead of our hand-rolled
// JsonDeserializer<T>. specific.avro.reader=true tells it to deserialize
// into the generated OrderEventAvro class (a SpecificRecord) rather than a
// generic Avro GenericRecord — same idea as JsonDeserializer's
// spring.json.value.default.type equivalent, just Avro's own mechanism.
public class AvroConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, OrderEventAvro> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("avro-orders-topic"));

            while (true) {
                ConsumerRecords<String, OrderEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, OrderEventAvro> record : records) {
                    OrderEventAvro order = record.value();
                    System.out.printf("partition=%d offset=%d key=%s order=%s%n",
                            record.partition(), record.offset(), record.key(), order);
                }
            }
        }
    }
}