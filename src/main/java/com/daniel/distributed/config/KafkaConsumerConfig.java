package com.daniel.distributed.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.daniel.distributed.kafka.DltSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Configures retry + dead-letter handling for Kafka listeners.
 *
 * <ul>
 *   <li>Transient failures are retried with exponential backoff from
 *       {@link KafkaConsumerRetryProperties}.</li>
 *   <li>Malformed / deserialization errors are not retried; they go straight to DLT.</li>
 *   <li>After the backoff budget is exhausted, the record is published to
 *       {@code {originalTopic}.DLT} and the original offset is committed so the
 *       partition is not blocked.</li>
 * </ul>
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

    private final KafkaConsumerRetryProperties retryProperties;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // Align with spring.kafka.listener.observation-enabled (W3C consumer spans).
        factory.getContainerProperties().setObservationEnabled(true);

        DeadLetterPublishingRecoverer recoverer = loggingDeadLetterRecoverer(kafkaTemplate);

        ExponentialBackOff backOff = new ExponentialBackOff(
            retryProperties.getInitialIntervalMs(),
            retryProperties.getMultiplier());
        backOff.setMaxElapsedTime(retryProperties.getMaxElapsedMs());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // Malformed payloads should not consume the full retry budget
        errorHandler.addNotRetryableExceptions(
            JsonProcessingException.class,
            SerializationException.class);

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Publishes to {@code topic.DLT} on the same partition index, with structured
     * logging before the publish. Does not mutate payment state.
     */
    static DeadLetterPublishingRecoverer loggingDeadLetterRecoverer(
            KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())) {

            @Override
            public void accept(ConsumerRecord<?, ?> record, Exception exception) {
                String paymentId = DltSupport.extractPaymentId(record);
                Throwable root = rootCause(exception);
                log.error(
                    "Publishing record to DLT: topic={} partition={} offset={} paymentId={} exceptionType={} message={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    paymentId != null ? paymentId : "unknown",
                    root != null ? root.getClass().getName() : exception.getClass().getName(),
                    root != null ? root.getMessage() : exception.getMessage());
                super.accept(record, exception);
            }
        };
        return recoverer;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur != null && cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }
}
