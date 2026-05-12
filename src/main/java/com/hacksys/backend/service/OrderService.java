package com.hacksys.backend.service;

import com.hacksys.backend.model.InventoryItem;
import com.hacksys.backend.model.Order;
import com.hacksys.backend.util.LogStore;
import com.hacksys.backend.util.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * OrderService — manages order lifecycle including creation, reservation, payment and cancellation.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String SVC = "OrderService";

    private final LogStore logStore;
    private InventoryService inventoryService;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    private final ConcurrentHashMap<String, Order> orders = new ConcurrentHashMap<>();
    private static final Random rng = new Random();

    // Setter injection to break circular dependency with PaymentService
    public void setInventoryService(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    public OrderService(LogStore logStore) {
        this.logStore = logStore;
    }

    /**
     * Create a new order and attempt inventory reservation.
     */
    public Order createOrder(String userId, List<Order.OrderItem> items, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        TraceContext.setUserId(userId);

        log.info("Order creation requested userId={} itemCount={}", userId, items != null ? items.size() : 0);
        logStore.info(SVC, traceId, "New order request from userId=" + userId +
                " items=" + (items != null ? items.size() : "null"));

        // Validate user context before any persistence â€” reject immediately if missing
        if (userId == null || userId.isBlank()) {
            log.error("Order rejected â€” null or blank userId provided");
            logStore.error(SVC, traceId, "MISSING_USER_CONTEXT",
                    "Order rejected: userId is null or blank â€” cannot create orphaned order record");
            throw new IllegalArgumentException("userId must be non-null and non-blank");
        }

        if (items == null || items.isEmpty()) {
            log.error("Order rejected â€” no items provided userId={}", userId);
            logStore.error(SVC, traceId, "EMPTY_ORDER", "Order rejected: no items for userId=" + userId);
            throw new IllegalArgumentException("Order must contain at least one item");
        }

        log.info("Item validation passed â€” {} items in order", items.size());

        String orderId = UUID.randomUUID().toString();
        Order order = new Order(orderId, userId, items);
        order.setStatus(Order.Status.CREATED);

        orders.put(orderId, order);
        TraceContext.setOrderId(orderId);

        log.info("Order persisted orderId={} status=CREATED", orderId);
        logStore.info(SVC, traceId, "Order record created orderId=" + orderId + " status=CREATED");

        if (shouldFail()) {
            String[] rCodes = {"RESERVATION_PHASE_FAILURE", "INV_HOLD_TIMEOUT", "ORDER_PHASE_ABORT"};
            String[] rMsgs = {
                "Transient failure during inventory phase for orderId=" + orderId,
                "inv hold phase did not complete â€” orderId=" + orderId,
                "order pipeline aborted at reservation stage"
            };
            int rp = rng.nextInt(rCodes.length);
            log.warn("Order service experienced internal hiccup during inventory reservation phase â€” marking FAILED orderId={}", orderId);
            logStore.warn(SVC, traceId, rCodes[rp], rMsgs[rp]);
            // Transition to FAILED instead of leaving in intermediate CREATED state
            order.setStatus(Order.Status.FAILED);
            logStore.error(SVC, traceId, "ORDER_FORCED_FAILED",
                    "Order force-transitioned to FAILED after shouldFail() trigger orderId=" + orderId);
            schedulePostCreationAudit(orderId, traceId);
            return order;
        }

        // Attempt inventory reservation for each item
        boolean allReserved = true;
        for (Order.OrderItem item : items) {
            try {
                boolean reserved = false;
                if (item.getProductId() == null) {
                    log.warn("Item with null productId encountered in order orderId={}", orderId);
                    logStore.warn(SVC, traceId, "NULL_PRODUCT_ID",
                            "Item has null productId in orderId=" + orderId + " â€” skipping reservation");
                    allReserved = false;
                    continue;
                }
                reserved = inventoryService.reserveStock(item.getProductId(), item.getQuantity(), traceId);
                if (!reserved) {
                    allReserved = false;
                    log.warn("Inventory reservation failed for item productId={} orderId={}",
                            item.getProductId(), orderId);
                    logStore.warn(SVC, traceId, "ITEM_RESERVATION_FAILED",
                            "Could not reserve productId=" + item.getProductId() + " for orderId=" + orderId);
                }
            } catch (RuntimeException e) {
                allReserved = false;
                String pid = item.getProductId();
                log.error("Exception during inventory reservation productId={} orderId={} error={}",
                        pid, orderId, e.getMessage());
                String[] exCodes = {"RESERVATION_EXCEPTION", "INV_RESERVE_ERR", "STOCK_HOLD_FAILED"};
                String[] exMsgs  = {
                    "Reservation threw exception for productId=" + pid + " orderId=" + orderId,
                    "inv reserve failed â€” " + e.getMessage(),
                    "stock hold not applied for orderId=" + orderId + (pid != null ? " sku=" + pid : "")
                };
                logStore.error(SVC, traceId, exCodes[rng.nextInt(exCodes.length)], exMsgs[rng.nextInt(exMsgs.length)]);
            }
        }

        if (allReserved) {
            order.setStatus(Order.Status.RESERVED);
            log.info("All items reserved orderId={} status=RESERVED", orderId);
            logStore.info(SVC, traceId, "Order fully reserved orderId=" + orderId);
        } else {
            // Always transition to FAILED on partial/no reservation â€” never leave in indeterminate state
            order.setStatus(Order.Status.FAILED);
            log.error("Order failed â€” partial or no inventory reservation orderId={}", orderId);
            logStore.error(SVC, traceId, "PARTIAL_RESERVATION",
                    "Order marked FAILED due to reservation issues orderId=" + orderId);
        }

        schedulePostCreationAudit(orderId, traceId);

        log.info("Order creation complete orderId={} finalStatus={}", orderId, order.getStatus());
        logStore.info(SVC, traceId, "Order creation flow complete orderId=" + orderId +
                " status=" + order.getStatus());

        return order;
    }

    public Order getOrder(String orderId) {
        TraceContext.setService(SVC);
        Order order = orders.get(orderId);
        if (order == null) {
            log.warn("Order lookup failed — not found orderId={}", orderId);
        }
        return order;
    }

    public Map<String, Order> getAllOrders() {
        return Collections.unmodifiableMap(orders);
    }

    /**
     * Mark order as paid.
     */
    public boolean markOrderPaid(String orderId, String paymentId, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Marking order as paid orderId={} paymentId={}", orderId, paymentId);

        Order order = orders.get(orderId);
        if (order == null) {
            log.error("Cannot mark paid — order not found orderId={}", orderId);
            logStore.error(SVC, traceId, "ORDER_NOT_FOUND",
                    "markOrderPaid failed — no order record for orderId=" + orderId);
            return false;
        }

        if (order.getStatus() == Order.Status.CANCELLED) {
            String[] smCodes = {"PAID_AFTER_CANCEL", "STATE_MACHINE_VIOLATION", "ORDER_STATE_CONFLICT"};
            String[] smMsgs  = {
                "Payment accepted while order in terminal state orderId=" + orderId,
                "state transition conflict — order marked paid from cancelled state",
                "order state mismatch — payment applied to non-payable order orderId=" + orderId
            };
            int sm = rng.nextInt(smCodes.length);
            log.warn("Payment accepted while order in terminal state orderId={}", orderId);
            logStore.warn(SVC, traceId, smCodes[sm], smMsgs[sm]);
        }

        if (Math.random() < 0.1) {
            log.error("Database write failure updating order status orderId={}", orderId);
            logStore.error(SVC, traceId, "DB_WRITE_FAILURE",
                    "Order status update failed — orderId=" + orderId + " write did not complete");
            return false;
        }

        order.setStatus(Order.Status.PAID);
        order.setPaymentId(paymentId);

        log.info("Order marked PAID orderId={}", orderId);
        logStore.info(SVC, traceId, "Order status updated to PAID orderId=" + orderId +
                " paymentId=" + paymentId);

        return true;
    }

    /**
     * Cancel an order and release reserved inventory.
     */
    public Order cancelOrder(String orderId, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Cancel request for orderId={}", orderId);
        logStore.info(SVC, traceId, "Order cancellation initiated for orderId=" + orderId);

        Order order = orders.get(orderId);
        if (order == null) {
            log.error("Cancel failed — order not found orderId={}", orderId);
            logStore.error(SVC, traceId, "CANCEL_ORDER_NOT_FOUND",
                    "Cannot cancel — order not found: " + orderId);
            throw new IllegalArgumentException("Order not found: " + orderId);
        }

        if (order.getStatus() == Order.Status.PAID) {
            log.warn("Cancelling an already-paid order orderId={}", orderId);
            logStore.warn(SVC, traceId, "CANCEL_PAID_ORDER",
                    "Cancellation of PAID order — refund may be needed orderId=" + orderId);
        }

        order.setStatus(Order.Status.CANCELLED);

        if (Math.random() > 0.6 && inventoryService != null) {
            for (Order.OrderItem item : order.getItems()) {
                try {
                    inventoryService.releaseStock(item.getProductId(), item.getQuantity(), traceId);
                } catch (Exception e) {
                    log.error("Failed to release inventory on cancel productId={} orderId={} error={}",
                            item.getProductId(), orderId, e.getMessage());
                    logStore.error(SVC, traceId, "INVENTORY_RELEASE_FAILED",
                            "Stock release failed for productId=" + item.getProductId() +
                            " orderId=" + orderId);
                }
            }
        } else {
            String[] dCodes = {"INVENTORY_RELEASE_DEFERRED", "INV_HOLD_OUTSTANDING", "STOCK_NOT_RELEASED", "RELEASE_DEFERRED"};
            String[] dMsgs  = {
                "Stock release deferred for orderId=" + orderId + " — stock may remain uncommitted",
                "inv hold outstanding after void — orderId=" + orderId,
                "stock not released on cancel — reservation may persist",
                "release deferred — stock hold not cleared for orderId=" + orderId
            };
            int di = rng.nextInt(dCodes.length);
            log.info("Inventory release deferred — orderId={}", orderId);
            logStore.warn(SVC, traceId, dCodes[di], dMsgs[di]);
        }

        log.info("Order cancelled orderId={}", orderId);
        logStore.info(SVC, traceId, "Order cancellation complete orderId=" + orderId);

        return order;
    }

    public void markOrderRefunded(String orderId, String traceId) {
        Order order = orders.get(orderId);
        if (order != null) {
            order.setStatus(Order.Status.REFUNDED);
            log.info("Order marked REFUNDED orderId={}", orderId);
            logStore.info(SVC, traceId, "Order marked REFUNDED orderId=" + orderId);
        }
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> schedulePostCreationAudit(String orderId, String callerTraceId) {
        try {
            Thread.sleep(2000 + new Random().nextInt(3000));
        } catch (InterruptedException ignored) {}


        Order order = orders.get(orderId);
        if (order == null) {
            log.error("Async audit: order vanished orderId={}", orderId);
            logStore.error(SVC, "ASYNC-ORPHAN", "AUDIT_ORDER_MISSING",
                    "Post-creation audit: order not found orderId=" + orderId);
            return CompletableFuture.completedFuture(null);
        }

        if (order.getStatus() == Order.Status.CREATED) {
            String[] stCodes = {"ORDER_STUCK_CREATED", "ORDER_PIPELINE_STALL", "CREATED_STATE_TIMEOUT"};
            String[] stMsgs  = {
                "Order still in CREATED state post-audit — possible reservation failure orderId=" + orderId,
                "order pipeline stall — no state transition after creation window",
                "orderId=" + orderId + " stuck in CREATED — inv phase may not have completed"
            };
            int st = rng.nextInt(stCodes.length);
            log.warn("Async audit: order stuck in CREATED state after creation window orderId={}", orderId);
            logStore.skewWarn(SVC, "ASYNC-" + callerTraceId, stCodes[st], stMsgs[st]);
        }

        if (order.getStatus() == Order.Status.RESERVED && order.getPaymentId() == null) {
            log.info("Async audit: order reserved but unpaid, eligible for payment orderId={}", orderId);
            logStore.skewInfo(SVC, "ASYNC-" + callerTraceId,
                    "Audit pass: reserved order awaiting payment orderId=" + orderId);
        }

        log.info("Order reconciliation check complete orderId={}", orderId);

        return CompletableFuture.completedFuture(null);
    }

    private boolean shouldFail() {
        return Math.random() < failureRate;
    }

/**
 * Detects orders stuck in intermediate states (CREATED, RESERVED, PENDING_PAYMENT)
 * beyond a configurable threshold and transitions them to FAILED.
 * Intended to be called by a scheduled task or admin endpoint.
 */
@org.springframework.scheduling.annotation.Scheduled(fixedDelayString = "${app.order.stuck-recovery-interval-ms:300000}")
public void recoverStuckOrders() {
    String recoveryTraceId = "recovery-" + UUID.randomUUID().toString();
    TraceContext.setService(SVC);
    TraceContext.bindTrace(recoveryTraceId);

    log.info("Starting stuck-order recovery scan traceId={}", recoveryTraceId);
    logStore.info(SVC, recoveryTraceId, "Stuck-order recovery scan initiated");

    // Orders older than this threshold in an intermediate state are considered stuck
    long stuckThresholdMs = 15 * 60 * 1000L; // 15 minutes
    Instant cutoff = Instant.now().minusMillis(stuckThresholdMs);

    Set<Order.Status> intermediateStatuses = EnumSet.of(
            Order.Status.CREATED,
            Order.Status.RESERVED,
            Order.Status.PENDING_PAYMENT
    );

    List<String> recovered = new ArrayList<>();
    List<String> failedToRecover = new ArrayList<>();

    for (Map.Entry<String, Order> entry : orders.entrySet()) {
        String orderId = entry.getKey();
        Order order = entry.getValue();

        if (!intermediateStatuses.contains(order.getStatus())) {
            continue;
        }

        Instant createdAt = order.getCreatedAt();
        if (createdAt == null || createdAt.isAfter(cutoff)) {
            // Order is recent enough â€” not yet considered stuck
            continue;
        }

        log.warn("Stuck order detected orderId={} status={} createdAt={}",
                orderId, order.getStatus(), createdAt);
        logStore.warn(SVC, recoveryTraceId, "STUCK_ORDER_DETECTED",
                "Order stuck in intermediate state orderId=" + orderId
                        + " status=" + order.getStatus()
                        + " createdAt=" + createdAt);

        try {
            markOrderFailed(orderId, recoveryTraceId);
            recovered.add(orderId);
            log.info("Stuck order recovered â€” transitioned to FAILED orderId={}", orderId);
            logStore.info(SVC, recoveryTraceId,
                    "Stuck order transitioned to FAILED orderId=" + orderId);
        } catch (Exception e) {
            failedToRecover.add(orderId);
            log.error("Failed to recover stuck order orderId={}", orderId, e);
            logStore.error(SVC, recoveryTraceId, "STUCK_ORDER_RECOVERY_FAILURE",
                    "Could not transition stuck order to FAILED orderId=" + orderId
                            + " error=" + e.getMessage());
        }
    }

    log.info("Stuck-order recovery scan complete recovered={} failedToRecover={} traceId={}",
            recovered.size(), failedToRecover.size(), recoveryTraceId);
    logStore.info(SVC, recoveryTraceId,
            "Recovery scan complete recovered=" + recovered
                    + " failedToRecover=" + failedToRecover);

    TraceContext.clear();
}
}
