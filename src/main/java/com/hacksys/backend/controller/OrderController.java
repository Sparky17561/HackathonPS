package com.hacksys.backend.controller;

import com.hacksys.backend.model.Order;
import com.hacksys.backend.service.OrderService;
import com.hacksys.backend.util.LogStore;
import com.hacksys.backend.util.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/order")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private static final String SVC = "OrderService";

    private final OrderService orderService;
    private final LogStore logStore;

    public OrderController(OrderService orderService, LogStore logStore) {
        this.orderService = orderService;
        this.logStore = logStore;
    }

    /**
     * POST /order
     * Body: { "userId": "...", "items": [{ "productId": "...", "quantity": N, "unitPrice": N }] }
     */
@PostMapping
public ResponseEntity<?> createOrder(@RequestBody Map<String, Object> body) {
    String traceId = TraceContext.initTrace();
    TraceContext.setService(SVC);

    log.info("POST /order received traceId={}", traceId);
    logStore.info(SVC, traceId, "POST /order request received");

    try {
        String userId = (String) body.get("userId");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rawItems = (List<Map<String, Object>>) body.get("items");

        if (rawItems == null || rawItems.isEmpty()) {
            logStore.error(SVC, traceId, "MISSING_ITEMS_FIELD",
                    "Request body missing or empty 'items' field");
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "items field required and must not be empty",
                    "trace_id", traceId));
        }

        List<Order.OrderItem> items = rawItems.stream().map(raw -> {
            String productId = (String) raw.get("productId");
            if (productId == null || productId.isBlank()) {
                throw new IllegalArgumentException(
                        "Each item must have a non-null, non-blank productId");
            }
            int qty = raw.containsKey("quantity")
                    ? ((Number) raw.get("quantity")).intValue() : 1;
            if (qty <= 0) {
                throw new IllegalArgumentException(
                        "Item quantity must be positive for productId=" + productId);
            }
            double price = raw.containsKey("unitPrice")
                    ? ((Number) raw.get("unitPrice")).doubleValue() : 0.0;
            return new Order.OrderItem(productId, qty, price);
        }).toList();

        Order order = orderService.createOrder(userId, items, traceId);

        // Surface INSUFFICIENT_STOCK as a 409 Conflict with correlation ID
        if (order.getStatus() == Order.Status.FAILED_INSUFFICIENT_STOCK) {
            log.warn("POST /order insufficient stock orderId={} traceId={}",
                    order.getId(), traceId);
            logStore.warn(SVC, traceId, "INSUFFICIENT_STOCK",
                    "Order rejected due to insufficient stock â€” orderId=" + order.getId());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "error", "INSUFFICIENT_STOCK",
                    "message", "One or more items could not be reserved due to insufficient stock",
                    "orderId", order.getId(),
                    "trace_id", traceId));
        }

        log.info("POST /order response orderId={} status={} traceId={}",
                order.getId(), order.getStatus(), traceId);
        return ResponseEntity.ok(Map.of(
                "orderId", order.getId(),
                "status", order.getStatus().name(),
                "trace_id", traceId));

    } catch (IllegalArgumentException e) {
        log.warn("POST /order bad request traceId={}: {}", traceId, e.getMessage());
        logStore.warn(SVC, traceId, "BAD_REQUEST",
                "Order rejected: " + e.getMessage());
        return ResponseEntity.badRequest().body(Map.of(
                "error", e.getMessage(),
                "trace_id", traceId));
    } catch (InventoryTransientException e) {
        log.warn("POST /order inventory transient failure traceId={}: {}",
                traceId, e.getMessage());
        logStore.warn(SVC, traceId, "INVENTORY_TRANSIENT_FAILURE",
                "Transient inventory failure â€” client should retry: " + e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                "error", "INVENTORY_TRANSIENT_FAILURE",
                "message", "Inventory service temporarily unavailable â€” please retry",
                "trace_id", traceId));
    } catch (Exception e) {
        log.error("POST /order unexpected error traceId={} exceptionType={} message={}",
                traceId, e.getClass().getName(), e.getMessage(), e);
        logStore.error(SVC, traceId, "UNHANDLED_ERROR",
                "Unexpected error in order creation: exceptionType=" + e.getClass().getName()
                + " message=" + e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "error", "Internal error",
                "trace_id", traceId));
    } finally {
        TraceContext.clearTrace();
    }
}

    /**
     * GET /order/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<?> getOrder(@PathVariable String id) {
        String traceId = TraceContext.initTrace();
        TraceContext.setService(SVC);

        log.info("GET /order/{} received", id);
        logStore.info(SVC, traceId, "GET /order/" + id);

        try {
            Order order = orderService.getOrder(id);
            if (order == null) {
                logStore.warn(SVC, traceId, "ORDER_NOT_FOUND", "Order not found: " + id);
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Order not found", "trace_id", traceId));
            }
            return ResponseEntity.ok(order);
        } finally {
            TraceContext.clearTrace();
        }
    }

    /**
     * POST /cancel
     * Body: { "orderId": "..." }
     */
    @PostMapping("/cancel")
    public ResponseEntity<?> cancelOrder(@RequestBody Map<String, Object> body) {
        String traceId = TraceContext.initTrace();
        TraceContext.setService(SVC);

        String orderId = (String) body.get("orderId");
        log.info("POST /cancel received orderId={}", orderId);
        logStore.info(SVC, traceId, "POST /cancel for orderId=" + orderId);

        try {
            if (orderId == null || orderId.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "orderId required", "trace_id", traceId));
            }
            Order order = orderService.cancelOrder(orderId, traceId);
            return ResponseEntity.ok(Map.of(
                    "orderId", order.getId(),
                    "status", order.getStatus().name(),
                    "trace_id", traceId
            ));
        } catch (IllegalArgumentException e) {
            logStore.error(SVC, traceId, "CANCEL_FAILED", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", e.getMessage(), "trace_id", traceId));
        } catch (Exception e) {
            log.error("POST /cancel error: {}", e.getMessage(), e);
            logStore.error(SVC, traceId, "UNHANDLED_ERROR", "Cancel error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal error", "trace_id", traceId));
        } finally {
            TraceContext.clearTrace();
        }
    }
}
