package com.hacksys.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableAsync
@EnableScheduling
public class BackendApplication {
public static void main(String[] args) {
    ConfigurableApplicationContext context =
            SpringApplication.run(BackendApplication.class, args);

    // Register JVM shutdown hook to handle SIGTERM/SIGKILL gracefully.
    // Triggers Spring context close which rolls back any active @Transactional
    // operations and flushes structured fatal log before process exit.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        org.slf4j.Logger shutdownLog =
                org.slf4j.LoggerFactory.getLogger(BackendApplication.class);
        try {
            shutdownLog.warn(
                    "{\"event\":\"SHUTDOWN_INITIATED\",\"service\":\"hacksys-backend\","
                    + "\"message\":\"JVM shutdown hook triggered â€” closing Spring context "
                    + "and rolling back in-flight inventory transactions\"}");

            // Attempt to flush any pending dead-letter / outbox entries
            if (context.isActive()) {
                try {
                    DeadLetterQueue dlq =
                            context.getBean(DeadLetterQueue.class);
                    dlq.flushPending();
                    shutdownLog.info(
                            "{\"event\":\"DLQ_FLUSHED\",\"service\":\"hacksys-backend\"}");
                } catch (Exception dlqEx) {
                    shutdownLog.error(
                            "{\"event\":\"DLQ_FLUSH_FAILED\",\"service\":\"hacksys-backend\","
                            + "\"exceptionType\":\"" + dlqEx.getClass().getName() + "\","
                            + "\"message\":\"" + dlqEx.getMessage() + "\"}");
                }
                // Close context â€” triggers @PreDestroy, DataSource cleanup,
                // and rolls back any active Spring-managed transactions
                context.close();
            }

            shutdownLog.warn(
                    "{\"event\":\"SHUTDOWN_COMPLETE\",\"service\":\"hacksys-backend\","
                    + "\"message\":\"Spring context closed cleanly\"}");
        } catch (Exception ex) {
            shutdownLog.error(
                    "{\"event\":\"SHUTDOWN_ERROR\",\"service\":\"hacksys-backend\","
                    + "\"exceptionType\":\"" + ex.getClass().getName() + "\","
                    + "\"message\":\"Error during shutdown hook â€” partial state possible: "
                    + ex.getMessage() + "\"}");
        }
    }, "inventory-shutdown-hook"));
}
}
