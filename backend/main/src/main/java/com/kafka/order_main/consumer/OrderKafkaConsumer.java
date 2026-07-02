package com.kafka.order_main.consumer;

import com.kafka.order_main.dto.OrderEvent;
import com.kafka.order_main.service.OrderStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderKafkaConsumer {

    private final OrderStoreService orderStoreService;

    @RetryableTopic(
            attempts = "${project.kafka.retry.attempts}",
            backoff = @Backoff(
                    delayExpression = "${project.kafka.retry.delay}",
                    multiplierExpression = "${project.kafka.retry.multiplier}"
            ),
            dltStrategy = DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "orders-main", groupId = "order-processor-group")
    public void consumeOrder(OrderEvent event) {
        log.info("📥 [KONSUMENT MAIN] Pobrano zdarzenie z kolejki. Event ID: {}, Order ID: {}", event.eventId(), event.orderId());

        if (event.amount().compareTo(BigDecimal.ZERO) < 0) {
            log.warn("❌ [WALIDACJA] Wykryto ujemną kwotę zamówienia: {}", event.amount());
            throw new IllegalArgumentException("Kwota zamówienia nie może być ujemna!");
        }

        orderStoreService.saveOrderToDatabase(event);
    }
}