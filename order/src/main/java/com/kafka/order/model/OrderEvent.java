package com.kafka.order.model;

import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    @Nonnull
    private String orderId;
    private OrderStatus orderStatus;
    private String orderAmount;
    @Nonnull
    private String customerId;
    private String productId;

    public enum OrderStatus {
        PENDING,
        CONFIRMED,
        SHIPPED,
        DELIVERED,
        CANCELLED
    }
}
