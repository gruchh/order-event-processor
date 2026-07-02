package com.kafka.order_main;

import com.kafka.order_main.dto.OrderEvent;
import com.kafka.order_main.model.Order;
import com.kafka.order_main.model.OrderStatus;
import com.kafka.order_main.model.Product;
import com.kafka.order_main.repository.OrderRepository;
import com.kafka.order_main.repository.ProductRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "project.kafka.retry.attempts=3",
        "project.kafka.retry.delay=500",
        "project.kafka.retry.multiplier=1.0"
})
class OrderProcessingFlowTest {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ProductRepository productRepository;

    @BeforeEach
    void setupDatabase() {
        orderRepository.deleteAll();
        productRepository.deleteAll();

        Product product = Product.builder()
                .name("Klawiatura Testowa")
                .sku("KEY-MECH-01")
                .basePrice(new BigDecimal("299.99"))
                .createdAt(Instant.now())
                .build();
        productRepository.save(product);
    }

    @Test
    @DisplayName("Gdy zamówienie jest poprawne -> przetwórz i zapisz ze statusem PROCESSED")
    void shouldProcessOrderSuccessfully() {
        String orderId = "ORD-TEST-OK-" + UUID.randomUUID();
        OrderEvent event = OrderEvent.create(orderId, "KEY-MECH-01", new BigDecimal("299.99"));

        kafkaTemplate.send("orders-main", orderId, event);

        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    var orderOpt = orderRepository.findByOrderId(orderId);
                    assertThat(orderOpt).isPresent();

                    Order savedOrder = orderOpt.get();
                    assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.PROCESSED);
                    assertThat(savedOrder.getAmount()).isEqualByComparingTo("299.99");
                    assertThat(savedOrder.getProduct().getSku()).isEqualTo("KEY-MECH-01");
                });
    }

    @Test
    @DisplayName("Gdy kwota jest ujemna -> wykonaj retry i prześlij paczkę do orders-main-dlt")
    void shouldRetryAndForwardToDltWhenAmountIsNegative() {
        String orderId = "ORD-TEST-ERR-" + UUID.randomUUID();
        OrderEvent invalidEvent = OrderEvent.create(orderId, "KEY-MECH-01", new BigDecimal("-150.00"));

        KafkaConsumer<String, String> dltConsumer = createDltTestConsumer();
        dltConsumer.subscribe(Collections.singletonList("orders-main-dlt"));

        kafkaTemplate.send("orders-main", orderId, invalidEvent);

        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    var records = dltConsumer.poll(Duration.ofMillis(500));
                    boolean containsOrder = false;
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.value().contains(orderId)) {
                            containsOrder = true;
                            break;
                        }
                    }
                    assertThat(containsOrder).as("Wiadomość powinna trafić do orders-main-dlt").isTrue();
                });

        assertThat(orderRepository.findAll()).isEmpty();

        dltConsumer.close();
    }

    private KafkaConsumer<String, String> createDltTestConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verification-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}