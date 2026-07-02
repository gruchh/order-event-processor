package com.kafka.order_main.controller;

import com.kafka.order_main.dto.CreateOrderRequest;
import com.kafka.order_main.dto.OrderEvent;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@CrossOrigin(origins = "http://localhost:4200")
public class OrderSimulatorController {

    private static final String ORDERS_TOPIC = "orders-main";
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @PostMapping("/simulate")
    public ResponseEntity<OrderEvent> simulateOrder(@RequestBody @Valid CreateOrderRequest request) {
        OrderEvent event = OrderEvent.create(
                request.orderId(),
                request.productSku(),
                request.amount()
        );

        log.info("🚀 [PRODUCER] Publikowanie zdarzenia na topic '{}': Order ID = {}, SKU = {}, Amount = {}",
                ORDERS_TOPIC, event.orderId(), event.productSku(), event.amount());

        kafkaTemplate.send(ORDERS_TOPIC, event.orderId(), event);

        return ResponseEntity.accepted().body(event);
    }

    @PostMapping("/publish-raw")
    public ResponseEntity<OrderEvent> publishRawEvent(@RequestBody @Valid OrderEvent event) {
        log.info("🚀 [PRODUCER-RAW] Publikowanie surowego zdarzenia: Event ID = {}, Order ID = {}", 
                event.eventId(), event.orderId());

        kafkaTemplate.send(ORDERS_TOPIC, event.orderId(), event);

        return ResponseEntity.accepted().body(event);
    }
}