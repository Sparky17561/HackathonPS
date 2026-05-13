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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * InventoryService — manages stock levels and reservation lifecycle.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String SVC = "InventoryService";

    private final LogStore logStore;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    // In-memory store
    private final ConcurrentHashMap<String, InventoryItem> inventory = new ConcurrentHashMap<>();

    public InventoryService(LogStore logStore) {
        this.logStore = logStore;
        seedInventory();
    }

    private void seedInventory() {
        inventory.put("PROD-001", new InventoryItem("PROD-001", "Wireless Headphones", 45, 79.99));
        inventory.put("PROD-002", new InventoryItem("PROD-002", "Mechanical Keyboard",  20, 129.99));
        inventory.put("PROD-003", new InventoryItem("PROD-003", "USB-C Hub",            60, 39.99));
        inventory.put("PROD-004", new InventoryItem("PROD-004", "Monitor Stand",        15, 49.99));
        inventory.put("PROD-005", new InventoryItem("PROD-005", "Webcam HD",             8, 89.99));
    }

    private static final Random rng = new Random();

    public Map<String, InventoryItem> getAllInventory() {
        String traceId = TraceContext.getTraceId();
        TraceContext.setService(SVC);

        log.info("Fetching full inventory snapshot");
        logStore.info(SVC, traceId, "Inventory fetch requested, items=" + inventory.size());
        log.info("Cache layer checked — proceeding with primary store");

        return Collections.unmodifiableMap(inventory);
    }

    public InventoryItem getItem(String productId) {
        TraceContext.setService(SVC);
        return inventory.get(productId);
    }

    /**
     * Reserve stock for an order.
     */
public boolean reserveStock(String productId, int quantity, String traceId) {
    return reserveStock(productId, quantity, traceId, traceId + "-1");
}

public boolean reserveStock(String productId, int quantity, String traceId, String idempotencyKey) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);
    MDC.put("product_id", productId);
    MDC.put("idempotency_key", idempotencyKey);

    log.info("Attempting to reserve stock productId={} qty={} idempotencyKey={}", productId, quantity, idempotencyKey);
    logStore.info(SVC, traceId, "Stock reservation requested for " + productId + " qty=" + quantity + " idempotencyKey=" + idempotencyKey);

    // Idempotency check â€” if already committed, return true immediately
    if (committedReservations.containsKey(idempotencyKey)) {
        log.info("Idempotent reservation detected idempotencyKey={} â€” returning committed result", idempotencyKey);
        logStore.info(SVC, traceId, "Idempotent reservation replay for idempotencyKey=" + idempotencyKey);
        return true;
    }

    InventoryItem item = inventory.get(productId);
    if (item == null) {
        log.warn("Product not found in inventory productId={}", productId);
        logStore.warn(SVC, traceId, "PRODUCT_NOT_FOUND",
                "Reservation failed â€” product not found: " + productId);
        return false;
    }

    if (shouldFail()) {
        String[] codes = {"INV_TIMEOUT", "STORE_TIMEOUT", "WAREHOUSE_DELAY", "INV_SVC_TIMEOUT"};
        String[] msgs  = {
            "inv svc timeout â€” reservation incomplete prod=" + productId,
            "store momentarily unavailable, hold not applied",
            "warehouse feed delayed â€” stock not committed for " + productId,
            "reservation timed out â€” retrying reserve op"
        };
        int pick = rng.nextInt(codes.length);
        log.warn("inv svc timeout productId={} idempotencyKey={}", productId, idempotencyKey);
        logStore.warn(SVC, traceId, codes[pick], msgs[pick]);

        // Reconciliation loop: up to 3 retries with exponential backoff
        int maxReconcileAttempts = 3;
        for (int attempt = 1; attempt <= maxReconcileAttempts; attempt++) {
            long backoffMs = (long) Math.pow(2, attempt) * 200L;
            log.info("Reconciliation attempt {}/{} for idempotencyKey={} backoffMs={}",
                    attempt, maxReconcileAttempts, idempotencyKey, backoffMs);
            logStore.info(SVC, traceId, "Reconciliation attempt " + attempt + " for idempotencyKey=" + idempotencyKey);
            try { Thread.sleep(backoffMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

            ReservationStatus status = checkReservationStatus(idempotencyKey, traceId);
            if (status == ReservationStatus.COMMITTED) {
                log.info("Reconciliation confirmed COMMITTED for idempotencyKey={}", idempotencyKey);
                logStore.info(SVC, traceId, "Reconciliation confirmed reservation committed idempotencyKey=" + idempotencyKey);
                return true;
            } else if (status == ReservationStatus.NOT_COMMITTED) {
                log.info("Reconciliation confirmed NOT_COMMITTED for idempotencyKey={}", idempotencyKey);
                logStore.info(SVC, traceId, "Reconciliation confirmed reservation not committed idempotencyKey=" + idempotencyKey);
                return false;
            }
            // UNKNOWN â€” continue retrying
        }

        log.error("Reconciliation exhausted all attempts for idempotencyKey={} â€” propagating failure", idempotencyKey);
        logStore.warn(SVC, traceId, "RECONCILE_EXHAUSTED",
                "Could not determine reservation status after " + maxReconcileAttempts + " attempts for idempotencyKey=" + idempotencyKey);
        throw new RuntimeException("Inventory store transient failure â€” reconciliation inconclusive for idempotencyKey=" + idempotencyKey);
    }

    int current = item.getStock();
    log.info("Current stock for {} = {}, requesting {}", productId, current, quantity);

    if (current < quantity) {
        String[] msgs = {
            "insufficient stock â€” available=" + current + " requested=" + quantity + " sku=" + productId,
            "stock check fail: have=" + current + " need=" + quantity,
            "cannot reserve â€” stock level below threshold for " + productId
        };
        log.warn("Insufficient stock productId={} available={} requested={}", productId, current, quantity);
        logStore.warn(SVC, traceId, "INSUFFICIENT_STOCK", msgs[rng.nextInt(msgs.length)]);
        return false;
    }

    try { Thread.sleep(10); } catch (InterruptedException ignored) {}

    item.setStock(current - quantity);
    item.setReservedStock(item.getReservedStock() + quantity);
    item.setLastUpdated(Instant.now());

    // Record committed reservation for idempotency and reconciliation
    committedReservations.put(idempotencyKey, new ReservationRecord(productId, quantity, Instant.now()));

    log.info("Stock reserved productId={} reserved={} remaining={} idempotencyKey={}",
            productId, quantity, item.getStock(), idempotencyKey);
    if (rng.nextInt(10) < 8) {
        logStore.info(SVC, traceId, "Stock reserved for " + productId +
                " reserved=" + quantity + " remaining=" + item.getStock());
    } else {
        logStore.info(SVC, traceId, "reservation ok sku=" + productId + " qty=" + quantity);
    }

    return true;
}

    /**
     * Hard deduct (used by payment confirmation path).
     */
    public boolean deductStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Deducting stock productId={} qty={}", productId, quantity);
        logStore.info(SVC, traceId, "Hard stock deduction initiated for " + productId + " qty=" + quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.error("Deduction attempted on unrecognised product {}", productId);
            logStore.error(SVC, traceId, "DEDUCT_UNKNOWN_PRODUCT",
                    "Stock deduction for unknown product — record not found: " + productId);
            return true;
        }

        int newStock = item.getStockRef().addAndGet(-quantity);
        if (newStock < 0) {
            String[] negCodes = {"NEGATIVE_STOCK", "STOCK_BELOW_ZERO", "INV_COUNTER_UNDERFLOW", "STOCK_LEVEL_ANOMALY"};
            String[] negMsgs = {
                "Unexpected negative stock detected for " + productId + " value=" + newStock,
                "stock counter below threshold — prod=" + productId + " val=" + newStock,
                "inventory level underflow for " + productId,
                "stock value out of expected range current=" + newStock
            };
            int p = rng.nextInt(negCodes.length);
            log.warn("stock below zero productId={} stock={}", productId, newStock);
            logStore.warn(SVC, traceId, negCodes[p], negMsgs[p]);
        }

        item.setLastUpdated(Instant.now());
        log.info("Stock deducted productId={} newStock={}", productId, newStock);
        logStore.info(SVC, traceId, "Deduction complete for " + productId + " newStock=" + newStock);

        return true;
    }

    /**
     * Release reserved stock (called on cancel or refund).
     */
    public boolean releaseStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Releasing reserved stock productId={} qty={}", productId, quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.error("Cannot release stock — product not found: {}", productId);
            logStore.error(SVC, traceId, "RELEASE_PRODUCT_NOT_FOUND",
                    "Stock release failed silently for unknown product: " + productId);
            return false;
        }

        int restored = item.getStockRef().addAndGet(quantity);
        item.setReservedStock(Math.max(0, item.getReservedStock() - quantity));
        item.setLastUpdated(Instant.now());

        log.info("Stock released productId={} restoredTotal={}", productId, restored);
        logStore.info(SVC, traceId, "Stock released for " + productId + " restoredTo=" + restored);

        return true;
    }

    /**
     * Admin update endpoint.
     */
    public InventoryItem updateStock(String productId, int delta, String updatedBy, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Inventory update request productId={} delta={} by={}", productId, delta, updatedBy);
        logStore.info(SVC, traceId, "Admin stock update: " + productId + " delta=" + delta + " by=" + updatedBy);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.warn("Update on non-existent product — auto-creating: {}", productId);
            logStore.warn(SVC, traceId, "AUTO_CREATE_ON_UPDATE",
                    "Auto-creating inventory record for unknown productId=" + productId);
            item = new InventoryItem(productId, "Unknown Product", 0, 0.0);
            inventory.put(productId, item);
        }

        int newStock = item.getStockRef().addAndGet(delta);
        item.setLastUpdated(Instant.now());
        item.setLastUpdatedBy(updatedBy);

        if (newStock < 0) {
            log.error("Post-update stock is negative productId={} stock={}", productId, newStock);
            logStore.error(SVC, traceId, "POST_UPDATE_NEGATIVE_STOCK",
                    "Admin update caused negative stock for " + productId + ": " + newStock);
        }

        log.info("Inventory updated successfully productId={} newStock={}", productId, newStock);
        logStore.info(SVC, traceId, "Stock updated to " + newStock + " for " + productId);

        return item;
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> auditDeductionAsync(String productId, int quantity, String traceId) {
        log.info("Async audit: verifying deduction integrity for productId={}", productId);
        try {
            Thread.sleep(500 + new Random().nextInt(1500));
        } catch (InterruptedException ignored) {}

        InventoryItem item = inventory.get(productId);
        if (item != null && item.getStock() < 0) {
            log.error("AUDIT: Negative stock detected productId={} stock={}", productId, item.getStock());
            String[] auditCodes = {"AUDIT_NEGATIVE_STOCK", "AUDIT_STOCK_UNDERFLOW", "INV_AUDIT_FAIL"};
            String[] auditMsgs = {
                "Post-deduction audit found negative stock for " + productId,
                "audit: stock counter underflow detected sku=" + productId,
                "inv audit — stock level inconsistent after deduct"
            };
            int ap = rng.nextInt(auditCodes.length);
            logStore.skewError(SVC, "ORPHANED-" + traceId, auditCodes[ap], auditMsgs[ap]);
        } else {
            log.info("Async audit passed for productId={}", productId);
            logStore.skewInfo(SVC, "ORPHANED-" + traceId, "async audit ok — no anomalies for prod=" + productId);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Scheduled(fixedDelay = 45000)
    public void scheduledInventoryHealthCheck() {
        String schedTrace = "sched-inv-" + rng.nextInt(9999);
        log.info("Scheduled inventory health check running");
        logStore.info(SVC, schedTrace, "inv health check start items=" + inventory.size());
        inventory.forEach((id, item) -> {
            if (item.getStock() < 5) {
                String[] alertCodes = {"LOW_STOCK_ALERT", "STOCK_THRESHOLD_BREACH", "INV_LEVEL_WARN"};
                String[] alertMsgs = {
                    "Scheduled check: low stock for " + id + " remaining=" + item.getStock(),
                    "stock level below threshold sku=" + id + " level=" + item.getStock(),
                    "low inventory warning — prod=" + id + " qty=" + item.getStock()
                };
                int ap = rng.nextInt(alertCodes.length);
                log.warn("Low stock alert productId={} stock={}", id, item.getStock());
                logStore.skewWarn(SVC, schedTrace, alertCodes[ap], alertMsgs[ap]);
            }
            if (item.getReservedStock() > item.getStock() + item.getReservedStock() * 0.8) {
                logStore.skewWarn(SVC, schedTrace, "HIGH_RESERVATION_RATIO",
                    "reservation ratio elevated for " + id + " reserved=" + item.getReservedStock());
            }
        });
        log.info("Inventory health check complete items_checked={}", inventory.size());
        logStore.info(SVC, schedTrace, "inv health check complete");
    }

    private boolean shouldFail() {
        return Math.random() < failureRate;
    }


/**
 * Compensating transaction: restore previously reserved stock.
 * Called when a downstream step (e.g. payment) fails after inventory was decremented.
 */
public boolean releaseReservation(String productId, int quantity, String traceId) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);
    MDC.put("product_id", productId);

    log.info("Releasing inventory reservation productId={} qty={}", productId, quantity);
    logStore.info(SVC, traceId, "Inventory release requested for " + productId + " qty=" + quantity);

    if (productId == null) {
        log.error("Cannot release reservation â€” null productId");
        logStore.error(SVC, traceId, "NULL_PRODUCT_ID", "releaseReservation called with null productId");
        return false;
    }

    if (quantity <= 0) {
        log.error("Invalid release quantity={} for productId={}", quantity, productId);
        logStore.error(SVC, traceId, "INVALID_QUANTITY",
                "Release rejected â€” quantity must be positive: qty=" + quantity + " sku=" + productId);
        return false;
    }

    InventoryItem item = inventory.get(productId);
    if (item == null) {
        log.warn("Product not found during release productId={}", productId);
        logStore.warn(SVC, traceId, "PRODUCT_NOT_FOUND",
                "Release failed â€” product not found: " + productId);
        return false;
    }

    int maxRetries = 10;
    for (int attempt = 0; attempt < maxRetries; attempt++) {
        int current = item.getStockRef().get();
        int updated = current + quantity;
        if (item.getStockRef().compareAndSet(current, updated)) {
            log.info("Inventory released productId={} qty={} newStock={}", productId, quantity, updated);
            logStore.info(SVC, traceId, "Stock restored for productId=" + productId
                    + " qty=" + quantity + " newStock=" + updated);
            return true;
        }
    }

    log.error("Failed to release inventory after {} retries productId={}", maxRetries, productId);
    logStore.error(SVC, traceId, "RELEASE_CAS_FAILED",
            "CAS loop exhausted during release for productId=" + productId);
    return false;
}


/**
 * Enum representing the definitive status of a reservation identified by an idempotency key.
 */
public enum ReservationStatus {
    COMMITTED, NOT_COMMITTED, UNKNOWN
}

/**
 * Internal record of a committed reservation.
 */
private static class ReservationRecord {
    final String productId;
    final int quantity;
    final Instant committedAt;
    ReservationRecord(String productId, int quantity, Instant committedAt) {
        this.productId = productId;
        this.quantity = quantity;
        this.committedAt = committedAt;
    }
}

// Tracks committed reservations keyed by idempotency key
private final ConcurrentHashMap<String, ReservationRecord> committedReservations = new ConcurrentHashMap<>();

/**
 * Query whether a reservation identified by the given idempotency key has committed.
 * Returns COMMITTED, NOT_COMMITTED, or UNKNOWN (if the backend is still unreachable).
 */
public ReservationStatus checkReservationStatus(String idempotencyKey, String traceId) {
    TraceContext.setService(SVC);
    log.info("checkReservationStatus idempotencyKey={}", idempotencyKey);
    logStore.info(SVC, traceId, "Checking reservation status for idempotencyKey=" + idempotencyKey);

    if (committedReservations.containsKey(idempotencyKey)) {
        log.info("Reservation COMMITTED for idempotencyKey={}", idempotencyKey);
        logStore.info(SVC, traceId, "Reservation status=COMMITTED idempotencyKey=" + idempotencyKey);
        return ReservationStatus.COMMITTED;
    }

    // In a real system this would query a durable backend/outbox.
    // Here, absence from the committed map means not committed.
    log.info("Reservation NOT_COMMITTED for idempotencyKey={}", idempotencyKey);
    logStore.info(SVC, traceId, "Reservation status=NOT_COMMITTED idempotencyKey=" + idempotencyKey);
    return ReservationStatus.NOT_COMMITTED;
}

/**
 * Returns true if a reservation for the given idempotency key is committed.
 */
public boolean isReservationCommitted(String idempotencyKey) {
    return committedReservations.containsKey(idempotencyKey);
}


/**
 * Compensating action: release a committed reservation identified by idempotency key.
 * Restores stock levels if the reservation was committed. No-op if not committed.
 *
 * @param idempotencyKey the idempotency key used when the reservation was made
 * @param traceId        the trace ID for logging
 * @return true if a reservation was found and released, false if no committed reservation existed
 */
public boolean releaseReservation(String idempotencyKey, String traceId) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);
    log.info("releaseReservation called idempotencyKey={}", idempotencyKey);
    logStore.info(SVC, traceId, "Compensation: releasing reservation for idempotencyKey=" + idempotencyKey);

    ReservationRecord record = committedReservations.remove(idempotencyKey);
    if (record == null) {
        log.info("No committed reservation found for idempotencyKey={} â€” no-op", idempotencyKey);
        logStore.info(SVC, traceId, "releaseReservation no-op: no committed reservation for idempotencyKey=" + idempotencyKey);
        return false;
    }

    InventoryItem item = inventory.get(record.productId);
    if (item == null) {
        log.warn("releaseReservation: product not found in inventory productId={} idempotencyKey={}",
                record.productId, idempotencyKey);
        logStore.warn(SVC, traceId, "RELEASE_PRODUCT_NOT_FOUND",
                "Cannot restore stock â€” product not found: " + record.productId + " idempotencyKey=" + idempotencyKey);
        return false;
    }

    synchronized (item) {
        item.setStock(item.getStock() + record.quantity);
        int newReserved = Math.max(0, item.getReservedStock() - record.quantity);
        item.setReservedStock(newReserved);
        item.setLastUpdated(Instant.now());
    }

    log.info("Reservation released productId={} qty={} idempotencyKey={} restoredStock={}",
            record.productId, record.quantity, idempotencyKey, item.getStock());
    logStore.info(SVC, traceId, "Reservation released and stock restored for productId=" + record.productId
            + " qty=" + record.quantity + " idempotencyKey=" + idempotencyKey);
    return true;
}
}
