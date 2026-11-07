package com.daniel.distributed.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.daniel.distributed.api.dto.DltReplayDtos.ReplayRequest;
import com.daniel.distributed.api.dto.DltReplayDtos.ReplayResponse;
import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.exception.InvalidDltReplayException;
import com.daniel.distributed.kafka.event.PaymentSubmittedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DltReplayServiceTest {

    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private SendResult<String, String> sendResult;

    private DltReplayService service;
    private SimpleMeterRegistry meterRegistry;
    private PaymentProperties properties;
    private ObjectMapper objectMapper;

    private UUID paymentId;
    private UUID fromId;
    private UUID toId;
    private String submittedPayload;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        properties = new PaymentProperties();
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        meterRegistry = new SimpleMeterRegistry();
        service = new DltReplayService(kafkaTemplate, properties, objectMapper, ObservationRegistry.NOOP,
            new PaymentLedgerMetrics(meterRegistry));

        paymentId = UUID.randomUUID();
        fromId = UUID.randomUUID();
        toId = UUID.randomUUID();
        PaymentSubmittedEvent event = new PaymentSubmittedEvent(
            paymentId, fromId, toId, new BigDecimal("25.00"), "USD", Instant.parse("2026-01-01T00:00:00Z"));
        submittedPayload = objectMapper.writeValueAsString(event);

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);
    }

    @Test
    void replaySubmitted_publishesToMainTopicWithFromAccountKey() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 1, 42L, fromId.toString(), submittedPayload);

        ReplayResponse response = service.replay(request);

        assertThat(response.mainTopic()).isEqualTo(properties.getSubmittedTopic());
        assertThat(response.key()).isEqualTo(fromId.toString());
        assertThat(response.paymentId()).isEqualTo(paymentId.toString());
        assertThat(response.status()).isEqualTo("PUBLISHED");

        verify(kafkaTemplate).send(
            eq(properties.getSubmittedTopic()),
            eq(fromId.toString()),
            eq(submittedPayload));
        verifyNoMoreInteractions(kafkaTemplate);
    }

    @Test
    void replaySubmitted_derivesKeyWhenOmitted() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 1L, null, submittedPayload);

        ReplayResponse response = service.replay(request);

        assertThat(response.key()).isEqualTo(fromId.toString());
        verify(kafkaTemplate).send(
            eq(properties.getSubmittedTopic()),
            eq(fromId.toString()),
            eq(submittedPayload));
    }

    @Test
    void replaySubmitted_wrongKey_rejected() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 1L, UUID.randomUUID().toString(), submittedPayload);

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class)
            .hasMessageContaining("fromAccountId");
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void replaySubmitted_malformedPayload_rejected() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 1L, fromId.toString(), "not-json");

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class);
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void replaySubmitted_missingPaymentId_rejected() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 1L, fromId.toString(), "{\"amount\":1}");

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class)
            .hasMessageContaining("paymentId");
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void unknownDltTopic_rejected() {
        ReplayRequest request = new ReplayRequest(
            "random.topic.DLT", 0, 1L, fromId.toString(), submittedPayload);

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class)
            .hasMessageContaining("Unknown");
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void mainTopicWithoutDltSuffix_rejected() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedTopic(), 0, 1L, fromId.toString(), submittedPayload);

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class);
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void multipleReplays_eachPublishOnce_samePayload() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 2, 99L, fromId.toString(), submittedPayload);

        service.replay(request);
        service.replay(request);

        verify(kafkaTemplate, times(2)).send(
            eq(properties.getSubmittedTopic()),
            eq(fromId.toString()),
            eq(submittedPayload));
    }

    @Test
    void replayDoesNotCreateNewPaymentId_payloadUnchanged() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 5L, fromId.toString(), submittedPayload);

        service.replay(request);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(properties.getSubmittedTopic()), eq(fromId.toString()), payloadCaptor.capture());
        assertThat(payloadCaptor.getValue()).isEqualTo(submittedPayload);
        assertThat(payloadCaptor.getValue()).contains(paymentId.toString());
    }

    @Test
    void resolveMainTopic_mapsAllKnownDlts() {
        assertThat(service.resolveMainTopic(properties.getSubmittedDltTopic()))
            .isEqualTo(properties.getSubmittedTopic());
        assertThat(service.resolveMainTopic(properties.getCompletedDltTopic()))
            .isEqualTo(properties.getCompletedTopic());
        assertThat(service.resolveMainTopic(properties.getFailedDltTopic()))
            .isEqualTo(properties.getFailedTopic());
    }

    @Test
    void kafkaSendFailure_recordsPublishFailed_notRejected() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 1L, fromId.toString(), submittedPayload);

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class);

        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "publish_failed").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "rejected").counter()).isNull();
        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "published").counter()).isNull();
    }

    @Test
    void invalidRequest_recordsRejected() {
        ReplayRequest request = new ReplayRequest(
            "not-a-dlt-topic", 0, 1L, fromId.toString(), submittedPayload);

        assertThatThrownBy(() -> service.replay(request))
            .isInstanceOf(InvalidDltReplayException.class);

        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "rejected").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "publish_failed").counter()).isNull();
    }

    @Test
    void successfulReplay_recordsPublished() {
        ReplayRequest request = new ReplayRequest(
            properties.getSubmittedDltTopic(), 0, 5L, fromId.toString(), submittedPayload);

        service.replay(request);

        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "published").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "rejected").counter()).isNull();
        assertThat(meterRegistry.find("payment_ledger.dlt.replay")
            .tag("outcome", "publish_failed").counter()).isNull();
    }
}
