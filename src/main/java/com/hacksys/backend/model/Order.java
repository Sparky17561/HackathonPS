package com.hacksys.backend.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Order {

    public enum Status {
        CREATED, RESERVED, PAID, FAILED, CANCELLED, REFUNDED
    }

    private String id;
    private String userId;
    // Intentional: using AtomicReference for "thread-safe" status but update is still non-atomic with reads
    private final AtomicReference<Status> status = new AtomicReference<>(Status.CREATED);
    private List<OrderItem> items;
    private Instant createdAt;
    private Instant updatedAt;
    // Intentional: no version/etag field — no optimistic locking
    private String paymentId;
    private String failureReason;

    public Order() {}

    public Order(String id, String userId, List<OrderItem> items) {
        this.id = id;
        this.userId = userId;
        this.items = items;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

public Status getStatus() { return status.get(); }

/**
 * Order lifecycle status enum.
 *
 * Terminal states:  CONFIRMED, CANCELLED, FAILED
 * Intermediate/stuck states: PENDING, RESERVATION_UNCERTAIN, RESERVATION_FAILED
 */
public enum Status {
    /** Order created, awaiting inventory reservation. */
    PENDING,
    /** Inventory reservation call timed out â€” outcome unknown; sweeper will reconcile. */
    RESERVATION_UNCERTAIN,
    /** Inventory reservation definitively failed (insufficient stock or hard error). */
    RESERVATION_FAILED,
    /** Inventory reserved; awaiting payment processing. */
    RESERVED,
    /** Payment processing in progress. */
    PAYMENT_PENDING,
    /** Order fully confirmed â€” inventory committed and payment captured. */
    CONFIRMED,
    /** Order cancelled â€” any held inventory has been released. */
    CANCELLED,
    /** Order failed due to an unrecoverable error. */
    FAILED
}
    public void setStatus(Status s) {
        this.status.set(s);
        this.updatedAt = Instant.now();
    }

    public List<OrderItem> getItems() { return items; }
    public void setItems(List<OrderItem> items) { this.items = items; }

    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OrderItem {
        private String productId;
        private int quantity;
        private double unitPrice;

        public OrderItem() {}
        public OrderItem(String productId, int quantity, double unitPrice) {
            this.productId = productId;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
        }

        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getUnitPrice() { return unitPrice; }
        public void setUnitPrice(double unitPrice) { this.unitPrice = unitPrice; }
    }
}
