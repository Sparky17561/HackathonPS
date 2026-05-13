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
public Order createOrder(String userId, List<Order.OrderItem> items, String traceId, String idempotencyKey) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);
    TraceContext.setUserId(userId);

    // Idempotency check â€” return existing result if this key was already processed
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
        Order existing = idempotencyMap.get(idempotencyKey);
        if (existing != null) {
            log.info("Idempotent request detected idempotencyKey={} returning existing orderId={}",
                    idempotencyKey, existing.getOrderId());
            logStore.info(SVC, traceId, "Idempotent replay â€” returning cached order for key=" + idempotencyKey
                    + " orderId=" + existing.getOrderId());
            return existing;
        }
    }

    log.info("Order creation requested userId={} itemCount={}", userId, items != null ? items.size() : 0);
    logStore.info(SVC, traceId, "New order request from userId=" + userId +
            " items=" + (items != null ? items.size() : "null"));

    if (items == null || items.isEmpty()) {
        log.error("Order rejected â€” no items provided userId={}", userId);
        logStore.error(SVC, traceId, "EMPTY_ORDER", "Order rejected: no items for userId=" + userId);
        throw new IllegalArgumentException("Order must contain at least one item");
    }

    if (userId == null) {
        log.warn("Order submitted with null userId â€” continuing without user association");
        logStore.warn(SVC, traceId, "NULL_USER_ID",
                "Order submitted without user context â€” downstream association unavailable");
    }
    log.info("Item validation passed â€” {} items in order", items.size());

    String orderId = UUID.randomUUID().toString();
    Order order = new Order(orderId, userId, items);
    order.setStatus(Order.Status.CREATED);
    order.setCreatedAt(Instant.now());

    orders.put(orderId, order);
    TraceContext.setOrderId(orderId);

    log.info("Order persisted orderId={} status=CREATED", orderId);
    logStore.info(SVC, traceId, "Order record created orderId=" + orderId + " status=CREATED");

    // Track which items were successfully reserved so we can roll back on failure
    List<Order.OrderItem> reservedItems = new ArrayList<>();
    boolean allReserved = true;

    for (Order.OrderItem item : items) {
        if (item.getProductId() == null) {
            log.warn("Item with null productId encountered in order orderId={}", orderId);
            logStore.warn(SVC, traceId, "NULL_PRODUCT_ID",
                    "Item has null productId in orderId=" + orderId + " â€” skipping reservation");
            allReserved = false;
            break;
        }
        try {
            boolean reserved = inventoryService.reserveStock(item.getProductId(), item.getQuantity(), traceId);
            if (reserved) {
                reservedItems.add(item);
            } else {
                allReserved = false;
                log.warn("Inventory reservation failed for item productId={} orderId={}",
                        item.getProductId(), orderId);
                logStore.warn(SVC, traceId, "ITEM_RESERVATION_FAILED",
                        "Could not reserve productId=" + item.getProductId() + " for orderId=" + orderId);
                break;
            }
        } catch (RuntimeException e) {
            allReserved = false;
            log.error("Exception during inventory reservation productId={} orderId={} error={}",
                    item.getProductId(), orderId, e.getMessage());
            logStore.error(SVC, traceId, "RESERVATION_EXCEPTION",
                    "Reservation threw exception for productId=" + item.getProductId()
                            + " orderId=" + orderId + " error=" + e.getMessage());
            break;
        }
    }

    if (!allReserved) {
        // Roll back any reservations already made
        for (Order.OrderItem reserved : reservedItems) {
            try {
                inventoryService.releaseReservation(reserved.getProductId(), reserved.getQuantity(), traceId);
                log.info("Rolled back reservation productId={} orderId={}", reserved.getProductId(), orderId);
                logStore.info(SVC, traceId, "Compensating release applied for productId="
                        + reserved.getProductId() + " orderId=" + orderId);
            } catch (RuntimeException re) {
                log.error("Failed to release reservation during rollback productId={} orderId={} error={}",
                        reserved.getProductId(), orderId, re.getMessage());
                logStore.error(SVC, traceId, "ROLLBACK_RELEASE_FAILED",
                        "Could not release reservation for productId=" + reserved.getProductId()
                                + " orderId=" + orderId + " â€” manual intervention required");
            }
        }
        order.setStatus(Order.Status.FAILED);
        log.error("Order failed â€” partial or no inventory reservation orderId={}", orderId);
        logStore.error(SVC, traceId, "PARTIAL_RESERVATION",
                "Order marked FAILED due to reservation issues orderId=" + orderId);
    } else {
        order.setStatus(Order.Status.RESERVED);
        log.info("All items reserved orderId={} status=RESERVED", orderId);
        logStore.info(SVC, traceId, "Order fully reserved orderId=" + orderId);
    }

    // Store idempotency result
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
        idempotencyMap.put(idempotencyKey, order);
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
 * Transitions the given order to {@link Order.Status#FAILED}, records the failure reason,
 * releases any held inventory, and emits an audit log event.
 *
 * @param orderId the ID of the order to fail
 * @param reason  a human-readable reason string (used for audit logging)
 * @throws IllegalArgumentException if the order does not exist
 */
public void markOrderFailed(String orderId, String reason) {
    Order order = orders.get(orderId);
    if (order == null) {
        log.error("markOrderFailed called for unknown orderId={}", orderId);
        logStore.error(SVC, reason, "ORDER_NOT_FOUND",
                "markOrderFailed: no order found for orderId=" + orderId);
        throw new IllegalArgumentException("Order not found: " + orderId);
    }

    Order.Status previousStatus = order.getStatus();

    // Idempotency guard â€” do not re-fail an already terminal order
    if (previousStatus == Order.Status.FAILED
            || previousStatus == Order.Status.CANCELLED
            || previousStatus == Order.Status.COMPLETED) {
        log.info("markOrderFailed skipped â€” order already in terminal state orderId={} status={}",
                orderId, previousStatus);
        return;
    }

    order.setStatus(Order.Status.FAILED);
    order.setFailureReason(reason);

    log.warn("Order transitioned to FAILED orderId={} previousStatus={} reason={}",
            orderId, previousStatus, reason);
    logStore.warn(SVC, reason, "ORDER_MARKED_FAILED",
            "Order transitioned to FAILED orderId=" + orderId
                    + " previousStatus=" + previousStatus
                    + " reason=" + reason);

    // Release inventory held during RESERVED or later stages
    if (previousStatus == Order.Status.RESERVED
            || previousStatus == Order.Status.PENDING_PAYMENT) {
        try {
            if (order.getItems() != null) {
                for (Order.OrderItem item : order.getItems()) {
                    if (item.getProductId() != null) {
                        inventoryService.releaseStock(item.getProductId(), item.getQuantity());
                        log.info("Inventory released for failed order orderId={} productId={} qty={}",
                                orderId, item.getProductId(), item.getQuantity());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to release inventory for failed order orderId={}", orderId, e);
            logStore.error(SVC, reason, "INVENTORY_RELEASE_FAILURE",
                    "Could not release inventory for orderId=" + orderId
                            + " error=" + e.getMessage());
        }
    }

    // Emit audit event
    logStore.info(SVC, reason,
            "AUDIT: order_failed orderId=" + orderId
                    + " previousStatus=" + previousStatus
                    + " failureReason=" + reason);
}


/**
 * Background job: every 5 minutes, find orders stuck in CREATED (non-terminal) state
 * for more than 10 minutes and transition them to FAILED with compensating transactions.
 */
@Scheduled(fixedDelay = 300_000)
public void recoverStuckOrders() {
    TraceContext.setService(SVC);
    String recoveryTraceId = UUID.randomUUID().toString();
    TraceContext.bindTrace(recoveryTraceId);

    Instant cutoff = Instant.now().minusSeconds(600); // 10 minutes
    log.info("Running stuck-order recovery job cutoff={}", cutoff);
    logStore.info(SVC, recoveryTraceId, "Stuck-order recovery job started cutoff=" + cutoff);

    for (Map.Entry<String, Order> entry : orders.entrySet()) {
        Order order = entry.getValue();
        // Only recover orders that are non-terminal and older than the cutoff
        if (order.getStatus() == Order.Status.FAILED
                || order.getStatus() == Order.Status.RESERVED
                || order.getStatus() == Order.Status.CANCELLED) {
            continue;
        }
        Instant createdAt = order.getCreatedAt();
        if (createdAt == null || createdAt.isAfter(cutoff)) {
            continue;
        }

        String orderId = order.getOrderId();
        log.warn("Recovering stuck order orderId={} status={} createdAt={}",
                orderId, order.getStatus(), createdAt);
        logStore.warn(SVC, recoveryTraceId, "STUCK_ORDER_RECOVERY",
                "Order stuck in non-terminal state â€” initiating recovery orderId=" + orderId
                        + " status=" + order.getStatus() + " createdAt=" + createdAt);

        // Release any inventory that may have been reserved
        List<Order.OrderItem> items = order.getItems();
        if (items != null) {
            for (Order.OrderItem item : items) {
                if (item.getProductId() == null) continue;
                try {
                    boolean released = inventoryService.releaseReservation(
                            item.getProductId(), item.getQuantity(), recoveryTraceId);
                    if (released) {
                        log.info("Recovery: released inventory productId={} orderId={}",
                                item.getProductId(), orderId);
                        logStore.info(SVC, recoveryTraceId,
                                "Recovery compensating release applied productId=" + item.getProductId()
                                        + " orderId=" + orderId);
                    }
                } catch (RuntimeException e) {
                    log.error("Recovery: failed to release inventory productId={} orderId={} error={}",
                            item.getProductId(), orderId, e.getMessage());
                    logStore.error(SVC, recoveryTraceId, "RECOVERY_RELEASE_FAILED",
                            "Could not release inventory during recovery productId=" + item.getProductId()
                                    + " orderId=" + orderId + " â€” manual intervention required");
                }
            }
        }

        order.setStatus(Order.Status.FAILED);
        order.setFailureReason("system_recovery");
        log.warn("Stuck order transitioned to FAILED orderId={} reason=system_recovery", orderId);
        logStore.warn(SVC, recoveryTraceId, "STUCK_ORDER_FAILED",
                "Order transitioned to FAILED by recovery job orderId=" + orderId
                        + " reason=system_recovery");
    }

    log.info("Stuck-order recovery job complete");
    logStore.info(SVC, recoveryTraceId, "Stuck-order recovery job finished");
}

// Idempotency map: idempotencyKey -> completed Order result
private final ConcurrentHashMap<String, Order> idempotencyMap = new ConcurrentHashMap<>();


/**
 * Saga failure handler: compensates a failed order by releasing any committed inventory
 * reservation, transitioning the order to FAILED, and restoring the user's cart.
 *
 * @param orderId        the order that failed
 * @param idempotencyKey the idempotency key used for the inventory reservation
 * @param traceId        the trace ID for logging
 */
public void handleOrderFailure(String orderId, String idempotencyKey, String traceId) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);
    log.info("handleOrderFailure orderId={} idempotencyKey={}", orderId, idempotencyKey);
    logStore.info(SVC, traceId, "Saga failure handler invoked for orderId=" + orderId
            + " idempotencyKey=" + idempotencyKey);

    Order order = orders.get(orderId);
    if (order == null) {
        log.warn("handleOrderFailure: order not found orderId={}", orderId);
        logStore.warn(SVC, traceId, "ORDER_NOT_FOUND",
                "Failure handler could not locate orderId=" + orderId);
        return;
    }

    // Guard: only compensate non-terminal orders
    if (order.getStatus() == Order.Status.FAILED || order.getStatus() == Order.Status.CONFIRMED) {
        log.info("handleOrderFailure: order already in terminal state orderId={} status={}",
                orderId, order.getStatus());
        logStore.info(SVC, traceId, "Order already terminal â€” skipping compensation orderId=" + orderId
                + " status=" + order.getStatus());
        return;
    }

    // Step 1: Release inventory reservation if it was committed
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
        try {
            boolean released = inventoryService.releaseReservation(idempotencyKey, traceId);
            if (released) {
                log.info("Inventory reservation released for orderId={} idempotencyKey={}",
                        orderId, idempotencyKey);
                logStore.info(SVC, traceId, "Compensation: inventory released for orderId=" + orderId
                        + " idempotencyKey=" + idempotencyKey);
            } else {
                log.info("No inventory reservation to release for orderId={} idempotencyKey={}",
                        orderId, idempotencyKey);
                logStore.info(SVC, traceId, "Compensation: no inventory reservation found â€” no-op for orderId=" + orderId);
            }
        } catch (Exception e) {
            log.error("Failed to release inventory reservation orderId={} idempotencyKey={} error={}",
                    orderId, idempotencyKey, e.getMessage(), e);
            logStore.warn(SVC, traceId, "RELEASE_RESERVATION_ERROR",
                    "Could not release reservation for orderId=" + orderId + ": " + e.getMessage());
        }
    }

    // Step 2: Transition order to FAILED terminal state
    order.setStatus(Order.Status.FAILED);
    order.setUpdatedAt(Instant.now());
    log.info("Order transitioned to FAILED orderId={}", orderId);
    logStore.info(SVC, traceId, "Order status=FAILED orderId=" + orderId);

    // Step 3: Restore cart items
    if (cartService != null) {
        try {
            cartService.restoreCart(orderId, traceId);
            log.info("Cart restored for orderId={}", orderId);
            logStore.info(SVC, traceId, "Cart restored for orderId=" + orderId);
        } catch (Exception e) {
            log.error("Failed to restore cart for orderId={} error={}", orderId, e.getMessage(), e);
            logStore.warn(SVC, traceId, "CART_RESTORE_ERROR",
                    "Cart restoration failed for orderId=" + orderId + ": " + e.getMessage());
        }
    } else {
        log.warn("CartService not available â€” cart not restored for orderId={}", orderId);
        logStore.warn(SVC, traceId, "CART_SERVICE_UNAVAILABLE",
                "CartService not wired â€” cart not restored for orderId=" + orderId);
    }
}


// CartService reference for cart restoration (setter-injected to avoid circular dependency)
private CartService cartService;

public void setCartService(CartService cartService) {
    this.cartService = cartService;
}

// Idempotency map for order creation
private final ConcurrentHashMap<String, Order> idempotencyMap = new ConcurrentHashMap<>();

// Tracks the idempotency key used for each order's inventory reservation
private final ConcurrentHashMap<String, String> orderIdempotencyKeys = new ConcurrentHashMap<>();

/**
 * Scheduled sweeper: detects orders stuck in non-terminal states older than 5 minutes
 * and triggers the saga compensation/failure handler.
 */
@org.springframework.scheduling.annotation.Scheduled(fixedDelay = 60_000)
public void processStuckOrders() {
    TraceContext.setService(SVC);
    String sweepTraceId = "sweeper-" + UUID.randomUUID();
    TraceContext.bindTrace(sweepTraceId);

    Instant cutoff = Instant.now().minusSeconds(300); // 5 minutes
    log.info("Stuck-order sweeper running cutoff={}", cutoff);
    logStore.info(SVC, sweepTraceId, "Stuck-order sweeper started cutoff=" + cutoff);

    List<Order> stuckOrders = orders.values().stream()
            .filter(o -> isNonTerminalStatus(o.getStatus()))
            .filter(o -> o.getCreatedAt() != null && o.getCreatedAt().isBefore(cutoff))
            .collect(java.util.stream.Collectors.toList());

    if (stuckOrders.isEmpty()) {
        log.info("Stuck-order sweeper: no stuck orders found");
        logStore.info(SVC, sweepTraceId, "Stuck-order sweeper: no stuck orders detected");
        return;
    }

    log.warn("Stuck-order sweeper found {} stuck order(s)", stuckOrders.size());
    logStore.warn(SVC, sweepTraceId, "STUCK_ORDERS_DETECTED",
            "Sweeper found " + stuckOrders.size() + " stuck order(s) older than 5 minutes");

    for (Order order : stuckOrders) {
        String orderId = order.getOrderId();
        String idempotencyKey = orderIdempotencyKeys.getOrDefault(orderId, orderId + "-1");
        log.warn("Processing stuck order orderId={} status={} createdAt={}",
                orderId, order.getStatus(), order.getCreatedAt());
        logStore.warn(SVC, sweepTraceId, "STUCK_ORDER_COMPENSATION",
                "Triggering compensation for stuck orderId=" + orderId
                        + " status=" + order.getStatus() + " createdAt=" + order.getCreatedAt());
        try {
            handleOrderFailure(orderId, idempotencyKey, sweepTraceId);
        } catch (Exception e) {
            log.error("Sweeper failed to compensate orderId={} error={}", orderId, e.getMessage(), e);
            logStore.warn(SVC, sweepTraceId, "SWEEPER_COMPENSATION_ERROR",
                    "Failed to compensate stuck orderId=" + orderId + ": " + e.getMessage());
        }
    }

    log.info("Stuck-order sweeper completed processed={}", stuckOrders.size());
    logStore.info(SVC, sweepTraceId, "Stuck-order sweeper completed processed=" + stuckOrders.size());
}

private boolean isNonTerminalStatus(Order.Status status) {
    return status != null
            && status != Order.Status.FAILED
            && status != Order.Status.CONFIRMED
            && status != Order.Status.CANCELLED;
}
}
