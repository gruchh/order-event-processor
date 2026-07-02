package com.kafka.order_main.service;

import com.kafka.order_main.dto.OrderEvent;
import com.kafka.order_main.model.Order;
import com.kafka.order_main.model.OrderStatus;
import com.kafka.order_main.model.Product;
import com.kafka.order_main.repository.OrderRepository;
import com.kafka.order_main.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderStoreService {

    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;

    @Transactional
    public void saveOrderToDatabase(OrderEvent event) {
        Product product = productRepository.findBySku(event.productSku())
                .orElseThrow(() -> new IllegalArgumentException("Nie znaleziono produktu ze SKU: " + event.productSku()));

        Order order = Order.builder()
                .orderId(event.orderId())
                .product(product)
                .amount(event.amount())
                .status(OrderStatus.PROCESSED)
                .createdAt(event.createdAt())
                .build();

        orderRepository.save(order);
        log.info("✅ [BAZA DANYCH] Zapisano zamówienie: {}", order.getOrderId());
    }
}