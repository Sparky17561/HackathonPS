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
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        MDC.put("product_id", productId);

        log.info("Attempting to reserve stock productId={} qty={}", productId, quantity);
        logStore.info(SVC, traceId, "Stock reservation requested for " + productId + " qty=" + quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.warn("Product not found in inventory productId={}", productId);
            logStore.warn(SVC, traceId, "PRODUCT_NOT_FOUND",
                    "Reservation failed — product not found: " + productId);
            return false;
        }

        if (shouldFail()) {
            String[] codes = {"INV_TIMEOUT", "STORE_TIMEOUT", "WAREHOUSE_DELAY", "INV_SVC_TIMEOUT"};
            String[] msgs  = {
                "inv svc timeout — reservation incomplete prod=" + productId,
                "store momentarily unavailable, hold not applied",
                "warehouse feed delayed — stock not committed for " + productId,
                "reservation timed out — retrying reserve op"
            };
            int pick = rng.nextInt(codes.length);
            log.warn("inv svc timeout productId={}", productId);
            logStore.warn(SVC, traceId, codes[pick], msgs[pick]);
            throw new RuntimeException("Inventory store transient failure");
        }

        int current = item.getStock();
        log.info("Current stock for {} = {}, requesting {}", productId, current, quantity);

        if (current < quantity) {
            String[] msgs = {
                "insufficient stock — available=" + current + " requested=" + quantity + " sku=" + productId,
                "stock check fail: have=" + current + " need=" + quantity,
                "cannot reserve — stock level below threshold for " + productId
            };
            log.warn("Insufficient stock productId={} available={} requested={}", productId, current, quantity);
            logStore.warn(SVC, traceId, "INSUFFICIENT_STOCK", msgs[rng.nextInt(msgs.length)]);
            return false;
        }

        try { Thread.sleep(10); } catch (InterruptedException ignored) {}

        item.setStock(current - quantity);
        item.setReservedStock(item.getReservedStock() + quantity);
        item.setLastUpdated(Instant.now());

        log.info("Stock reserved productId={} reserved={} remaining={}", productId, quantity, item.getStock());
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
 * Query the reservation status for a given idempotency key.
 * Returns a map with keys: "status" (CONFIRMED | NOT_FOUND | PENDING),
 * and optionally "reservationId".
 * Used by saga compensation logic to determine whether a timed-out
 * reservation was actually applied.
 */
public Map<String, String> getReservationStatus(String idempotencyKey, String traceId) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);

    log.info("getReservationStatus idempotencyKey={}", idempotencyKey);
    logStore.info(SVC, traceId,
        "getReservationStatus: querying reservation for idempotencyKey=" + idempotencyKey);

    if (idempotencyKey == null || idempotencyKey.isBlank()) {
        logStore.warn(SVC, traceId, "IDEMPOTENCY_KEY_MISSING",
            "getReservationStatus: idempotency key is null or blank");
        Map<String, String> result = new HashMap<>();
        result.put("status", "NOT_FOUND");
        return result;
    }

    // Check local idempotency table (in-memory store keyed by idempotency key)
    Map<String, String> record = idempotencyTable.get(idempotencyKey);
    if (record != null) {
        String status = record.getOrDefault("status", "NOT_FOUND");
        log.info("getReservationStatus: found local record idempotencyKey={} status={}",
            idempotencyKey, status);
        logStore.info(SVC, traceId,
            "getReservationStatus: local record found idempotencyKey=" + idempotencyKey
            + " status=" + status);
        return Collections.unmodifiableMap(record);
    }

    // No local record â€” treat as NOT_FOUND (reservation was never committed)
    log.info("getReservationStatus: no record found for idempotencyKey={}", idempotencyKey);
    logStore.info(SVC, traceId,
        "getReservationStatus: no record found idempotencyKey=" + idempotencyKey
        + " returning NOT_FOUND");
    Map<String, String> notFound = new HashMap<>();
    notFound.put("status", "NOT_FOUND");
    return Collections.unmodifiableMap(notFound);
}

/**
 * Reserve stock with idempotency key support.
 * Records the reservation attempt in the local idempotency table before
 * executing, so that duplicate calls return the original result.
 */
public boolean reserveStockIdempotent(String productId, int quantity,
                                      String idempotencyKey, String traceId) {
    TraceContext.setService(SVC);
    TraceContext.bindTrace(traceId);

    log.info("reserveStockIdempotent idempotencyKey={} productId={} qty={}",
        idempotencyKey, productId, quantity);
    logStore.info(SVC, traceId,
        "reserveStockIdempotent: checking idempotency table key=" + idempotencyKey);

    // Check idempotency table first
    Map<String, String> existing = idempotencyTable.get(idempotencyKey);
    if (existing != null) {
        String status = existing.getOrDefault("status", "NOT_FOUND");
        logStore.info(SVC, traceId,
            "reserveStockIdempotent: duplicate call detected key=" + idempotencyKey
            + " returning cached status=" + status);
        return "CONFIRMED".equals(status);
    }

    // Record PENDING before outbound call
    Map<String, String> pendingRecord = new HashMap<>();
    pendingRecord.put("status", "PENDING");
    pendingRecord.put("productId", productId);
    pendingRecord.put("quantity", String.valueOf(quantity));
    pendingRecord.put("traceId", traceId);
    pendingRecord.put("createdAt", String.valueOf(System.currentTimeMillis()));
    idempotencyTable.put(idempotencyKey, pendingRecord);

    try {
        boolean result = reserveStock(productId, quantity, traceId);
        // Update idempotency table with final status
        Map<String, String> finalRecord = new HashMap<>(pendingRecord);
        finalRecord.put("status", result ? "CONFIRMED" : "FAILED");
        finalRecord.put("reservationId", idempotencyKey + "-res");
        idempotencyTable.put(idempotencyKey, finalRecord);
        logStore.info(SVC, traceId,
            "reserveStockIdempotent: recorded result key=" + idempotencyKey
            + " status=" + finalRecord.get("status"));
        return result;
    } catch (RuntimeException e) {
        // Leave as PENDING so compensation can query and resolve
        logStore.warn(SVC, traceId, "INV_TIMEOUT",
            "reserveStockIdempotent: reservation threw exception key=" + idempotencyKey
            + " leaving status=PENDING for compensation: " + e.getMessage());
        throw e;
    }
}

// Local idempotency table: idempotencyKey -> {status, reservationId, ...}
private final ConcurrentHashMap<String, Map<String, String>> idempotencyTable =
    new ConcurrentHashMap<>();
}
