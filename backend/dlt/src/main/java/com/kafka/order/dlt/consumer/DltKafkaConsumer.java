package com.kafka.order.dlt.consumer;

import com.kafka.order.dlt.dto.OrderEvent;
import com.kafka.order.dlt.service.DltLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class DltKafkaConsumer {

    private final DltLogService dltLogService;

    @KafkaListener(topics = "orders-main.DLT", groupId = "order-dlt-manager-group")
    public void consumeDlt(OrderEvent event, 
                           @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage) {
        log.error("🚨 [KONSUMENT DLT] Przechwycono niepoprawną paczkę z kolejki orders-main.DLT!");
        log.error("-> ID Zamówienia: {}, Powód awarii: {}", event.orderId(), exceptionMessage);

        dltLogService.logFailedEvent(event, exceptionMessage);
    }
}