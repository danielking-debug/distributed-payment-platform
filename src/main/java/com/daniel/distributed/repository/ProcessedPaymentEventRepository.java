package com.daniel.distributed.repository;

import com.daniel.distributed.domain.ProcessedPaymentEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedPaymentEventRepository extends JpaRepository<ProcessedPaymentEvent, UUID> {
}
