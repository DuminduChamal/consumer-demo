package com.learning.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// Baseline consumer: the simplest possible poll loop — subscribe, poll
// forever, print. Default auto-commit (enable.auto.commit=true), no
// manual offset control. Every other consumer in this project is a
// variation that changes exactly one thing relative to this one — commit
// strategy, shutdown handling, or deserialization — so this is the
// reference point to compare them against. The ConsumerRebalanceListener
// below is only here to make partition assignment visible when running
// multiple instances under the same group.id.
public class SimpleConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Consumers in the same group.id share a topic's partitions between them.
        // With just one consumer, this one gets all partitions.
        // group.id is new — producers don't have this concept. It's what makes this a consumer group: Kafka tracks progress (committed offsets) per group,
        // and if you ran two instances with the same group.id, they'd split keyed-topic's 3 partitions between them instead of both reading everything.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "learning-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // No offsets committed yet for this group -> start from the beginning
        // of the topic, same as --from-beginning on the CLI consumer.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("keyed-topic"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("Partitions revoked: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Partitions assigned: " + partitions);
                }
            });

            System.out.println("Polling for messages... (Ctrl+C to stop)");
            while (true) {
                // poll() blocks up to this long waiting for records, then
                // returns whatever arrived (possibly nothing).
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
}