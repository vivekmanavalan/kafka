# Kafka Order Processing System

This is a Spring Boot application that demonstrates Kafka integration for order processing. The system uses Kafka for event-driven order processing with proper partitioning and consumer group management.

## Features

- Order event production and consumption
- Topic partitioning (3 partitions)
- Consumer group management
- Spring Boot integration
- REST API endpoints for order operations

## Prerequisites

- Java 17 or higher
- Maven
- Kafka 3.x
- Spring Boot 3.x

## Configuration

### Kafka Configuration
- Topic: `order-event`
- Partitions: 3
- Replication Factor: 1 (for single-node setup)

### Application Properties
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.group-id=order-group
spring.kafka.consumer.auto-offset-reset=earliest
```

## Getting Started

1. Start Kafka server
2. Run the application:
   ```bash
   mvn spring-boot:run
   ```

## API Endpoints

- POST `/api/orders` - Create a new order
- GET `/api/orders/{id}` - Get order by ID

## Project Structure

```
src/main/java/com/kafka/order/
├── config/         # Kafka and application configuration
├── controller/     # REST controllers
├── model/         # Data models
├── service/       # Business logic
└── Constants/     # Application constants
```

## License

MIT License 