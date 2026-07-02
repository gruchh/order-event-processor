package com.kafka.order.dlt.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderEvent(
        UUID eventId,
        String orderId,
        String productSku,
        BigDecimal amount,
        Instant createdAt
) {}