package com.daniel.distributed.api;

import com.daniel.distributed.api.dto.ReconciliationDtos.RunRequest;
import com.daniel.distributed.api.dto.ReconciliationDtos.RunResponse;
import com.daniel.distributed.service.ReconciliationService;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Manual operations endpoint for payment/ledger reconciliation.
 * Always returns HTTP 200 with findings in the body when the run succeeds.
 */
@RestController
@RequestMapping("/api/v1/ops/reconciliation")
@RequiredArgsConstructor
public class ReconciliationController {

    private final ReconciliationService reconciliationService;

    @Operation(
        summary = "Run payment/ledger reconciliation",
        description = "Read-only scan of payments vs ledger entries and processed_payment_events. "
            + "Does not repair data or move money. Findings in the response are data, not HTTP errors.")
    @PostMapping("/run")
    public ResponseEntity<RunResponse> run(@RequestBody(required = false) RunRequest request) {
        RunResponse response = reconciliationService.run(request != null ? request : new RunRequest(null, null, null));
        return ResponseEntity.ok(response);
    }
}
