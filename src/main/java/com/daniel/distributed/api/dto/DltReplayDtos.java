package com.daniel.distributed.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Request/response for controlled manual DLT replay.
 * Operator supplies the DLT record fields (as read from Kafka); the service
 * validates and republishes to the corresponding main topic.
 */
public final class DltReplayDtos {

    private DltReplayDtos() {}

    public record ReplayRequest(
        @NotBlank String dltTopic,
        @NotNull Integer partition,
        @NotNull Long offset,
        /** Kafka record key; for payments.submitted must be fromAccountId. */
        String key,
        @NotBlank String payload
    ) {}

    public record ReplayResponse(
        String dltTopic,
        int partition,
        long offset,
        String mainTopic,
        String key,
        String paymentId,
        String status,
        String message
    ) {}
}
