package com.daniel.distributed.repository;

import com.daniel.distributed.domain.Payment;
import com.daniel.distributed.domain.PaymentStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    Page<Payment> findAllByOrderByCreatedAtDesc(Pageable pageable);

    List<Payment> findByCreatedAtGreaterThanEqual(Instant createdAt);

    List<Payment> findByStatusAndCreatedAtLessThanEqual(PaymentStatus status, Instant createdAt);
}
