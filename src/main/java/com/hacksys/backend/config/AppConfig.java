package com.hacksys.backend.config;

import com.hacksys.backend.service.InventoryService;
import com.hacksys.backend.service.OrderService;
import org.springframework.context.annotation.Configuration;
import jakarta.annotation.PostConstruct;

@Configuration
public class AppConfig {

    private final OrderService orderService;
    private final InventoryService inventoryService;

    public AppConfig(OrderService orderService, InventoryService inventoryService) {
        this.orderService = orderService;
        this.inventoryService = inventoryService;
    }

    /**
     * Break circular dependency: OrderService ↔ InventoryService.
     * Injected via setter post-construction.
     */
    @PostConstruct
    public void wireServices() {
        orderService.setInventoryService(inventoryService);
    }


/**
 * Registers a JVM-wide uncaught exception handler that logs all unhandled
 * Throwable instances as ERROR with full stack trace and request context,
 * and attempts to transition any in-flight order to FAILED state before
 * the thread terminates, ensuring failures are never silently swallowed.
 */
@PostConstruct
public void globalExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
        // Derive as much context as possible from the thread name
        String threadName = thread.getName();
        String orderId = "UNKNOWN";
        String userId = "UNKNOWN";
        String operation = threadName;

        // Attempt to extract orderId / userId encoded in thread name by convention
        // e.g. thread names like "order-proc-<orderId>-<userId>"
        try {
            if (threadName.contains("order-proc-")) {
                String[] parts = threadName.split("-");
                if (parts.length >= 4) {
                    orderId = parts[2];
                    userId = parts[3];
                }
            }
        } catch (Exception ignored) {
            // best-effort context extraction â€” never let this handler itself throw
        }

        // Build a structured log message with full stack trace
        StringBuilder sb = new StringBuilder();
        sb.append("GLOBAL_UNCAUGHT_EXCEPTION");
        sb.append(" thread=").append(threadName);
        sb.append(" orderId=").append(orderId);
        sb.append(" userId=").append(userId);
        sb.append(" operation=").append(operation);
        sb.append(" exceptionType=").append(throwable.getClass().getName());
        sb.append(" message=").append(throwable.getMessage());

        // Append full stack trace
        sb.append(" stackTrace=[");
        for (StackTraceElement ste : throwable.getStackTrace()) {
            sb.append(ste.toString()).append(" | ");
        }
        sb.append("]");

        // Log via SLF4J so it reaches all configured appenders
        org.slf4j.LoggerFactory.getLogger(AppConfig.class)
            .error("[GLOBAL_UNCAUGHT_EXCEPTION] " + sb, throwable);

        // Attempt to transition the in-flight order to FAILED state
        if (!"UNKNOWN".equals(orderId)) {
            try {
                orderService.failOrder(orderId, "GLOBAL_UNCAUGHT_EXCEPTION: " + throwable.getMessage());
                org.slf4j.LoggerFactory.getLogger(AppConfig.class)
                    .error("[GLOBAL_UNCAUGHT_EXCEPTION] orderId={} transitioned to FAILED", orderId);
            } catch (Exception compensationEx) {
                org.slf4j.LoggerFactory.getLogger(AppConfig.class)
                    .error("[GLOBAL_UNCAUGHT_EXCEPTION] failed to transition orderId={} to FAILED: {}",
                        orderId, compensationEx.getMessage(), compensationEx);
            }

            // Release any inventory reservation held by this order
            try {
                inventoryService.releaseReservation(orderId, "global-exception-handler");
                org.slf4j.LoggerFactory.getLogger(AppConfig.class)
                    .error("[GLOBAL_UNCAUGHT_EXCEPTION] inventory reservation released for orderId={}", orderId);
            } catch (Exception invEx) {
                org.slf4j.LoggerFactory.getLogger(AppConfig.class)
                    .error("[GLOBAL_UNCAUGHT_EXCEPTION] failed to release inventory for orderId={}: {}",
                        orderId, invEx.getMessage(), invEx);
            }
        }
    });

    org.slf4j.LoggerFactory.getLogger(AppConfig.class)
        .info("[AppConfig] Global uncaught exception handler registered");
}
}
