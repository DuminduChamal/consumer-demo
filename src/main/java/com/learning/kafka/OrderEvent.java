package com.learning.kafka;

public class OrderEvent {
    private String orderId;
    private double amount;
    private long timestamp;

    public OrderEvent() {}

    public OrderEvent(String orderId, double amount, long timestamp) {
        this.orderId = orderId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getOrderId() { return orderId; }
    public double getAmount() { return amount; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "OrderEvent{orderId='" + orderId + "', amount=" + amount + ", timestamp=" + timestamp + "}";
    }
}