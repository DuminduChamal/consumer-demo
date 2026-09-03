package com.learning.kafka.consumer;

import com.learning.kafka.deserializer.JsonDeserializer;
import com.learning.kafka.dto.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// Change vs. SimpleConsumer: deserializes into an actual Java object
// (OrderEvent) via the custom JsonDeserializer below, instead of a String.
// Reads orders-topic specifically (not keyed-topic) because keyed-topic has
// mixed message formats from other examples — a JSON deserializer throws on
// a non-JSON record and kills the poll loop, so JSON traffic gets its own
// topic. A topic should generally carry one consistent message schema.
public class JsonConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_CLASS_CONFIG, OrderEvent.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("orders-topic"));

            while (true) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, OrderEvent> record : records) {
                    OrderEvent order = record.value();
                    System.out.printf("partition=%d offset=%d key=%s order=%s%n",
                            record.partition(), record.offset(), record.key(), order);
                }
            }
        }
    }
}