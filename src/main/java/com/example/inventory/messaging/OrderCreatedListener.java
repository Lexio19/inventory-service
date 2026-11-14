package com.example.inventory.messaging;

import com.example.inventory.event.OrderCreatedEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(OrderCreatedListener.class);

    private final ObjectMapper objectMapper;

    public OrderCreatedListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}")
    public void consume(ConsumerRecord<String, String> record) {

        try {

            // Obtenemos el JSON recibido desde Kafka
            String message = record.value();

            // Convertimos el JSON en nuestro objeto Java
            OrderCreatedEvent event = objectMapper.readValue(
                    message,
                    OrderCreatedEvent.class
            );

            LOGGER.info("========================================");
            LOGGER.info("ORDER CREATED EVENT RECEIVED");
            LOGGER.info("Topic: {}", record.topic());
            LOGGER.info("Partition: {}", record.partition());
            LOGGER.info("Offset: {}", record.offset());
            LOGGER.info("Event ID: {}", event.getEventId());
            LOGGER.info("Order ID: {}", event.getOrderId());
            LOGGER.info("Product: {}", event.getProductName());
            LOGGER.info("Quantity: {}", event.getQuantity());
            LOGGER.info("Occurred At: {}", event.getOccurredAt());
            LOGGER.info("========================================");

        } catch (JsonProcessingException ex) {

            LOGGER.error("Invalid JSON received from Kafka", ex);

            throw new IllegalArgumentException(
                    "Cannot deserialize OrderCreatedEvent",
                    ex
            );
        }
    }
}