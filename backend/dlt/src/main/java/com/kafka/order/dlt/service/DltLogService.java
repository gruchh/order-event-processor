package com.kafka.order.dlt.service;

import com.kafka.order.dlt.dto.OrderEvent;
import com.kafka.order.dlt.model.FailedEvent;
import com.kafka.order.dlt.model.DltStatus;
import com.kafka.order.dlt.repository.FailedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
@Service
public class DltLogService {

    private final FailedEventRepository failedEventRepository;

    @Transactional
    public void logFailedEvent(OrderEvent event, String exceptionMessage) {
        log.info("💾 Zapisuję zatrute zdarzenie do bazy DLT. Order ID: {}", event.orderId());

        FailedEvent failedEvent = FailedEvent.builder()
                .eventId(event.eventId())
                .orderId(event.orderId())
                .productSku(event.productSku())
                .amount(event.amount())
                .exceptionMessage(exceptionMessage != null ? exceptionMessage : "Unknown Kafka Error")
                .status(DltStatus.NEW)
                .createdAt(Instant.now())
                .build();

        failedEventRepository.save(failedEvent);
        log.info("💀 [DLT SAVED] Zdarzenie błędu dla zamówienia {} zalogowane w bazie.", event.orderId());
    }
}