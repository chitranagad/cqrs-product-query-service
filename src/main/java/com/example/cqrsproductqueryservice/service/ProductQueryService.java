package com.example.cqrsproductqueryservice.service;

import com.example.cqrsproductqueryservice.dto.ProductEvent;
import com.example.cqrsproductqueryservice.entity.Product;
import com.example.cqrsproductqueryservice.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class ProductQueryService {

    // Initialise the SLF4J Logger for this class
    private static final Logger log = LoggerFactory.getLogger(ProductQueryService.class);

    @Autowired
    private ProductRepository productRepository;

    public List<Product> getProducts() {
        return productRepository.findAll();
    }

    @KafkaListener(topics = "product-event-topic", groupId = "product-query-service")
    public void processProductEvent(ProductEvent productEvent) {
        // Defensive check: Skip if payload or nested product is null
        if (productEvent == null || productEvent.getProduct() == null) {
            log.warn("Skipping null or malformed product event received from Kafka topic.");
            return;
        }

        Product product = productEvent.getProduct();
        String eventType = productEvent.getEventType();

        log.info("Processing event: {} for Product ID: {}", eventType, product.getId());

        if ("CreateProduct".equalsIgnoreCase(eventType)) {
            productRepository.save(product);
            log.info("Successfully created product ID: {}", product.getId());

        } else if ("UpdateProduct".equalsIgnoreCase(eventType)) {
            Optional<Product> optionalProduct = productRepository.findById(product.getId());

            if (optionalProduct.isPresent()) {
                Product existingProduct = optionalProduct.get();

                // Update ALL product fields to keep databases identical
                existingProduct.setName(product.getName());
                existingProduct.setPrice(product.getPrice());
                existingProduct.setDescription(product.getDescription()); // Maps description field

                productRepository.save(existingProduct);
                log.info("Successfully updated product ID: {}", product.getId());
            } else {
                // Upsert Fallback: If database was cleared, save it as new instead of getting stuck
                productRepository.save(product);
                log.warn("Product missing in read DB during update. Executed upsert fallback for ID: {}", product.getId());
            }
        }
    }
}
