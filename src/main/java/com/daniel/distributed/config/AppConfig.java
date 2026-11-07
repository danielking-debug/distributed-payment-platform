package com.daniel.distributed.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
    PaymentProperties.class,
    OutboxProperties.class,
    KafkaConsumerRetryProperties.class,
    ReconciliationProperties.class
})
public class AppConfig {
}
