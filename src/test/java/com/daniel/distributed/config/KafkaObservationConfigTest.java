package com.daniel.distributed.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies the listener container factory enables Micrometer observation so
 * Spring Kafka can extract W3C trace context from consumer records.
 */
class KafkaObservationConfigTest {

    @Test
    void listenerContainerFactory_enablesObservation() {
        KafkaConsumerRetryProperties retry = new KafkaConsumerRetryProperties();
        KafkaConsumerConfig config = new KafkaConsumerConfig(retry);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            config.kafkaListenerContainerFactory(
                new DefaultKafkaConsumerFactory<>(props),
                template);

        assertThat(factory.getContainerProperties().isObservationEnabled())
            .as("consumer observation must be on for W3C header extraction")
            .isTrue();
    }
}
