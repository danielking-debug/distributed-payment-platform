package com.daniel.distributed.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerRetryPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(EnableRetryProps.class)
        .withPropertyValues(
            "app.kafka.consumer.retry.initial-interval-ms=250",
            "app.kafka.consumer.retry.multiplier=3.0",
            "app.kafka.consumer.retry.max-elapsed-ms=5000");

    @Test
    void bindsFromApplicationProperties() {
        contextRunner.run(ctx -> {
            KafkaConsumerRetryProperties props = ctx.getBean(KafkaConsumerRetryProperties.class);
            assertThat(props.getInitialIntervalMs()).isEqualTo(250L);
            assertThat(props.getMultiplier()).isEqualTo(3.0);
            assertThat(props.getMaxElapsedMs()).isEqualTo(5000L);
        });
    }

    @Test
    void defaultValues_matchDocumentedDefaults() {
        new ApplicationContextRunner()
            .withUserConfiguration(EnableRetryProps.class)
            .run(ctx -> {
                KafkaConsumerRetryProperties props = ctx.getBean(KafkaConsumerRetryProperties.class);
                assertThat(props.getInitialIntervalMs()).isEqualTo(500L);
                assertThat(props.getMultiplier()).isEqualTo(2.0);
                assertThat(props.getMaxElapsedMs()).isEqualTo(10_000L);
            });
    }

    @Test
    void exponentialBackOff_usesBoundValuesAndEventuallyStops() {
        KafkaConsumerRetryProperties props = new KafkaConsumerRetryProperties();
        props.setInitialIntervalMs(100);
        props.setMultiplier(2.0);
        props.setMaxElapsedMs(500);

        ExponentialBackOff backOff = new ExponentialBackOff(props.getInitialIntervalMs(), props.getMultiplier());
        backOff.setMaxElapsedTime(props.getMaxElapsedMs());

        BackOffExecution exec = backOff.start();
        long first = exec.nextBackOff();
        assertThat(first).isEqualTo(100L);

        boolean stopped = false;
        for (int i = 0; i < 20; i++) {
            long next = exec.nextBackOff();
            if (next == BackOffExecution.STOP) {
                stopped = true;
                break;
            }
        }
        assertThat(stopped).isTrue();
    }

    @EnableConfigurationProperties(KafkaConsumerRetryProperties.class)
    static class EnableRetryProps {
    }
}
