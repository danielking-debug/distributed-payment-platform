package com.daniel.distributed.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares main and dead-letter topics for every topic consumed by the app.
 * DLT naming follows {@code {topic}.DLT} (same convention as
 * {@link KafkaConsumerConfig}'s recoverer).
 */
@Configuration
public class KafkaTopicConfig {

    @Autowired
    private PaymentProperties paymentProperties;

    @Bean
    public NewTopic paymentsSubmittedTopic() {
        return TopicBuilder.name(paymentProperties.getSubmittedTopic())
            .partitions(paymentProperties.getPartitions())
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentsCompletedTopic() {
        return TopicBuilder.name(paymentProperties.getCompletedTopic())
            .partitions(paymentProperties.getPartitions())
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentsFailedTopic() {
        return TopicBuilder.name(paymentProperties.getFailedTopic())
            .partitions(paymentProperties.getPartitions())
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentsSubmittedDltTopic() {
        return TopicBuilder.name(paymentProperties.getSubmittedDltTopic())
            .partitions(paymentProperties.getPartitions())
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentsCompletedDltTopic() {
        return TopicBuilder.name(paymentProperties.getCompletedDltTopic())
            .partitions(paymentProperties.getPartitions())
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentsFailedDltTopic() {
        return TopicBuilder.name(paymentProperties.getFailedDltTopic())
            .partitions(paymentProperties.getPartitions())
            .replicas(1)
            .build();
    }
}
