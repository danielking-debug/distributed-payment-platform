package com.daniel.distributed.api;

import com.daniel.distributed.api.dto.PaymentDtos.CreatePaymentRequest;
import com.daniel.distributed.api.dto.PaymentDtos.PaymentResponse;
import com.daniel.distributed.service.PaymentQueryService;
import com.daniel.distributed.service.PaymentSubmissionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentSubmissionService paymentSubmissionService;
    private final PaymentQueryService paymentQueryService;

    @Operation(summary = "Submit a payment",
        description = "Accepted immediately and processed asynchronously through Kafka. "
            + "Idempotency-Key is required; retries with the same key and same payload "
            + "return the original payment. The same key with a different payload returns 409.")
    @PostMapping
    public ResponseEntity<PaymentResponse> submitPayment(
        @Valid @RequestBody CreatePaymentRequest request,
        @Parameter(description = "Client-generated key bound to this request payload; required", required = true)
        @RequestHeader(value = "Idempotency-Key", required = true) String idempotencyKey) {

        PaymentResponse response = paymentSubmissionService.submit(request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @Operation(summary = "Get payment status")
    @GetMapping("/{paymentId}")
    public PaymentResponse getPayment(@PathVariable UUID paymentId) {
        return paymentQueryService.getPayment(paymentId);
    }

    @Operation(summary = "List recent payments across all accounts, most recent first")
    @GetMapping
    public Page<PaymentResponse> listPayments(
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {
        Pageable pageable = PageRequest.of(page, Math.min(size, 100), Sort.by(Sort.Direction.DESC, "createdAt"));
        return paymentQueryService.listPayments(pageable);
    }
}
