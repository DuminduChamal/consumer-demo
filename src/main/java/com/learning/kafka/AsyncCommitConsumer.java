package com.learning.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// Change vs. ManualCommitConsumer: commitSync() (blocking) replaced with
// commitAsync() (non-blocking, with a callback) so the poll loop never
// stalls on a broker round-trip. Also adds the standard graceful-shutdown
// idiom: a JVM shutdown hook calls consumer.wakeup() — the one
// KafkaConsumer method safe to call from another thread — to interrupt a
// blocked poll() via WakeupException, then a final commitSync() runs in a
// finally block, since a clean shutdown has no "next" async commit coming
// along to fix a missed one.
public class AsyncCommitConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "async-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Ctrl+C runs this on a separate thread; wakeup() is the only
        // KafkaConsumer method safe to call from outside the polling thread.
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(Collections.singletonList("keyed-topic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            System.err.println("Async commit FAILED: " + exception.getMessage());
                        } else {
                            System.out.println("Async commit succeeded: " + offsets);
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            // Expected — this is how the shutdown hook breaks the poll loop.
        } finally {
            try {
                consumer.commitSync(); // guaranteed final commit, nothing to supersede it now
                System.out.println("Final commitSync done — safe to exit.");
            } finally {
                consumer.close();
            }
        }
    }
}