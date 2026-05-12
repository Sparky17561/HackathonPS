java.util.Set
java.util.List
java.time.temporal.ChronoUnit
java.time.Instant
org.springframework.scheduling.annotation.Scheduled
org.slf4j.LoggerFactory
org.slf4j.Logger
com.hacksys.backend.model.Order
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
}
