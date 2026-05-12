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

        log.info("POST /order received");
        logStore.info(SVC, traceId, "POST /order request received");

        try {
            String userId = (String) body.get("userId");

            // Reject immediately at the controller boundary if userId is absent
            if (userId == null || userId.isBlank()) {
                log.warn("POST /order rejected â€” missing or blank userId");
                logStore.error(SVC, traceId, "MISSING_USER_ID",
                        "Order request rejected at controller: userId is null or blank");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                        .body(Map.of("error", "userId is required and must be non-blank", "trace_id", traceId));
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rawItems = (List<Map<String, Object>>) body.get("items");

            if (rawItems == null) {
                logStore.error(SVC, traceId, "MISSING_ITEMS_FIELD", "Request body missing 'items' field");
                return ResponseEntity.badRequest().body(Map.of("error", "items field required", "trace_id", traceId));
            }

            List<Order.OrderItem> items = rawItems.stream().map(raw -> {
                String productId = (String) raw.get("productId"); // can be null â€” intentional
                int qty = raw.containsKey("quantity") ? ((Number) raw.get("quantity")).intValue() : 1;
                double price = raw.containsKey("unitPrice") ? ((Number) raw.get("unitPrice")).doubleValue() : 0.0;
                return new Order.OrderItem(productId, qty, price);
            }).toList();

            Order order = orderService.createOrder(userId, items, traceId);

            log.info("POST /order response orderId={} status={}", order.getId(), order.getStatus());
            return ResponseEntity.ok(Map.of(
                    "orderId", order.getId(),
                    "status", order.getStatus().name(),
                    "trace_id", traceId
            ));

        } catch (IllegalArgumentException e) {
            log.warn("POST /order bad request: {}", e.getMessage());
            logStore.warn(SVC, traceId, "BAD_REQUEST", "Order rejected: " + e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage(), "trace_id", traceId));
        } catch (Exception e) {
            log.error("POST /order unexpected error: {}", e.getMessage(), e);
            logStore.error(SVC, traceId, "UNHANDLED_ERROR", "Unexpected error in order creation: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal error", "trace_id", traceId));
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
