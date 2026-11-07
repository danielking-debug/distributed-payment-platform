package com.daniel.distributed.api;

import com.daniel.distributed.api.dto.DltReplayDtos.ReplayRequest;
import com.daniel.distributed.api.dto.DltReplayDtos.ReplayResponse;
import com.daniel.distributed.service.DltReplayService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator endpoint for <em>manual</em> DLT replay. No UI; no automatic replay.
 * Republishes a validated record to the main topic; the normal consumer processes it.
 */
@RestController
@RequestMapping("/api/v1/ops/dlt")
@RequiredArgsConstructor
public class DltReplayController {

    private final DltReplayService dltReplayService;

    @Operation(
        summary = "Manually replay a DLT record onto its main topic",
        description = "Validates the payload and republishes to the original main topic "
            + "with the correct Kafka key. Does not create a new payment or call the "
            + "ledger processor directly. Processing is idempotent via the normal consumer path.")
    @PostMapping("/replay")
    public ResponseEntity<ReplayResponse> replay(@Valid @RequestBody ReplayRequest request) {
        ReplayResponse response = dltReplayService.replay(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
