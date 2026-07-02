package com.kafka.order.dlt.service;

import com.kafka.order.dlt.dto.OrderEvent;
import com.kafka.order.dlt.model.FailedOrderLog;
import com.kafka.order.dlt.repository.FailedOrderLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
@Service
public class DltLogService {

    private final FailedOrderLogRepository failedOrderLogRepository;

    @Transactional
    public void logFailedEvent(OrderEvent event, String exceptionMessage) {
        log.info("💾 Zapisuję zatrute zdarzenie do bazy DLT. Order ID: {}", event.orderId());

        FailedOrderLog failedLog = FailedOrderLog.builder()
                .eventId(event.eventId())
                .orderId(event.orderId())
                .productSku(event.productSku())
                .amount(event.amount())
                .originalTopic("orders-main")
                .exceptionMessage(exceptionMessage != null ? exceptionMessage : "Unknown Kafka Error")
                .failedAt(Instant.now())
                .build();

        failedOrderLogRepository.save(failedLog);
        log.info("💀 [DLT SAVED] Zdarzenie błędu dla zamówienia {} zalogowane w bazie.", event.orderId());
    }
}