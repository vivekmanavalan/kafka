package com.kafka.order.model;

import com.kafka.order.model.OrderEvent.OrderStatus;

import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderStream {
    @Nonnull 
    private String orderId;
    private OrderStatus orderStatus;
    private int orderAmount;
}
