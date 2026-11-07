package com.daniel.distributed.kafka;

import com.daniel.distributed.config.OutboxProperties;
import com.daniel.distributed.domain.OutboxEvent;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import com.daniel.distributed.observability.PaymentTracing;
import com.daniel.distributed.repository.OutboxEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxPublisherObservationTest {

    @Mock OutboxEventRepository outboxEventRepository;
    @Mock KafkaTemplate<String, String> kafkaTemplate;
    @Mock SendResult<String, String> sendResult;

    OutboxProperties outboxProperties = new OutboxProperties();
    TestObservationRegistry observationRegistry;
    OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        observationRegistry = TestObservationRegistry.create();
        publisher = new OutboxPublisher(
            outboxEventRepository, kafkaTemplate, outboxProperties, observationRegistry,
            new PaymentLedgerMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void successfulPublish_recordsSuccessfulObservation_andMarksPublished() {
        OutboxEvent event = event("payments.submitted", "payload-ok");
        when(outboxEventRepository.findUnpublishedBatchForUpdate(any(Integer.class)))
            .thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("payments.submitted"), anyString(), eq("payload-ok")))
            .thenReturn(CompletableFuture.completedFuture(sendResult));

        publisher.publishPendingEvents();

        assertThat(event.getPublishedAt()).isNotNull();
        verify(outboxEventRepository).save(event);
        TestObservationRegistryAssert.assertThat(observationRegistry)
            .hasNumberOfObservationsWithNameEqualTo(PaymentTracing.OUTBOX_PUBLISH, 1)
            .hasObservationWithNameEqualTo(PaymentTracing.OUTBOX_PUBLISH)
            .that()
            .hasBeenStarted()
            .hasBeenStopped()
            .doesNotHaveError();
    }

    @Test
    void failedPublish_recordsObservationError_andLeavesUnpublished() {
        OutboxEvent event = event("payments.submitted", "payload-fail");
        when(outboxEventRepository.findUnpublishedBatchForUpdate(any(Integer.class)))
            .thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        publisher.publishPendingEvents();

        assertThat(event.getPublishedAt()).isNull();
        verify(outboxEventRepository, times(0)).save(any());
        TestObservationRegistryAssert.assertThat(observationRegistry)
            .hasObservationWithNameEqualTo(PaymentTracing.OUTBOX_PUBLISH)
            .that()
            .hasBeenStarted()
            .hasBeenStopped()
            .hasError();
    }

    @Test
    void failedEvent_doesNotPreventLaterEventsInBatch() {
        OutboxEvent fail = event("payments.submitted", "fail");
        OutboxEvent ok = event("payments.submitted", "ok");
        when(outboxEventRepository.findUnpublishedBatchForUpdate(any(Integer.class)))
            .thenReturn(List.of(fail, ok));
        when(kafkaTemplate.send(anyString(), anyString(), eq("fail")))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        when(kafkaTemplate.send(anyString(), anyString(), eq("ok")))
            .thenReturn(CompletableFuture.completedFuture(sendResult));

        publisher.publishPendingEvents();

        assertThat(fail.getPublishedAt()).isNull();
        assertThat(ok.getPublishedAt()).isNotNull();
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), eq("fail"));
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), eq("ok"));
        verify(outboxEventRepository, times(1)).save(ok);

        TestObservationRegistryAssert.assertThat(observationRegistry)
            .hasNumberOfObservationsWithNameEqualTo(PaymentTracing.OUTBOX_PUBLISH, 2);
    }

    private static OutboxEvent event(String topic, String payload) {
        return OutboxEvent.builder()
            .id(UUID.randomUUID())
            .aggregateType("Payment")
            .aggregateId(UUID.randomUUID())
            .eventType("PaymentSubmitted")
            .topic(topic)
            .partitionKey(UUID.randomUUID().toString())
            .payload(payload)
            .build();
    }
}
