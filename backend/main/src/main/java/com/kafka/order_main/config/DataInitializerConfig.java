package com.kafka.order_main.config;

import com.kafka.order_main.model.Product;
import com.kafka.order_main.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.time.Instant;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class DataInitializerConfig {

    private final ProductRepository productRepository;

    @Bean
    public CommandLineRunner initDatabase() {
        return args -> {
            if (productRepository.findBySku("KEY-MECH-01").isEmpty()) {
                Product product = Product.builder()
                        .name("Klawiatura Mechaniczna RGB")
                        .sku("KEY-MECH-01")
                        .basePrice(new BigDecimal("299.99"))
                        .createdAt(Instant.now())
                        .build();

                productRepository.save(product);
                log.info("🛒 [DATA SEED] Zainicjalizowano domyślny produkt: KEY-MECH-01");
            }
        };
    }
}