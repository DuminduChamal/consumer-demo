package com.learning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

// Generic Deserializer<T> used by JsonConsumer for any target type, decoding
// via Jackson's ObjectMapper. Since Java generics are erased at runtime,
// Kafka can't infer T from the type parameter alone — the target class is
// read from the config map in configure() instead (Kafka instantiates this
// via a no-arg constructor, then calls configure() with the full Properties).
public class JsonDeserializer<T> implements Deserializer<T> {

    public static final String VALUE_CLASS_CONFIG = "json.deserializer.value.class";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetType;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Kafka instantiates this with a no-arg constructor, then calls
        // configure() with everything from Properties — this is how we
        // learn which class to deserialize into.
        String className = (String) configs.get(VALUE_CLASS_CONFIG);
        try {
            targetType = (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Unknown target class: " + className, e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON", e);
        }
    }
}