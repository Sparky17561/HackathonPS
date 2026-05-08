package com.hacksys.backend.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Thread-safe in-memory log store.
 * Backs the /logs API endpoint.
 * Capped at 2000 entries — older entries silently dropped.
 * 
 * Automatically captures the calling class name using stack trace analysis.
 */
@Component
public class LogStore {

    private static final Logger log = LoggerFactory.getLogger(LogStore.class);
    private static final int MAX_ENTRIES = 2000;
    private static final ObjectMapper mapper = new ObjectMapper();

    // Static so it survives bean re-creation during context refresh
    private static final CopyOnWriteArrayList<ObjectNode> entries = new CopyOnWriteArrayList<>();

    /**
     * Automatically detects the calling class name from the stack trace.
     * Skips frames in LogStore and common utility classes to find the real caller.
     */
    private static String getCallingClassName() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        
        // Walk the stack to find the first frame outside LogStore and common frameworks
        for (StackTraceElement element : stackTrace) {
            String className = element.getClassName();
            
            // Skip LogStore, Thread, and framework internals
            if (className.equals("java.lang.Thread") ||
                className.startsWith("java.lang.StackTraceElement") ||
                className.equals("com.hacksys.backend.util.LogStore") ||
                className.startsWith("java.") ||
                className.startsWith("sun.")) {
                continue;
            }
            
            // Return the first real application class
            return className;
        }
        
        return "unknown";
    }

    public void append(String level, String service, String traceId,
                       String errorType, String message, String className) {
        ObjectNode node = mapper.createObjectNode();
        node.put("timestamp", Instant.now().toString());
        node.put("trace_id", traceId != null ? traceId : "unknown");
        node.put("service", service != null ? service : "unknown");
        
        // Use provided className if available, otherwise auto-detect
        String resolvedClassName = (className != null) ? className : getCallingClassName();
        node.put("class", resolvedClassName);
        
        node.put("error_type", errorType != null ? errorType : "NONE");
        node.put("message", message);
        node.put("level", level);

        entries.add(node);

        // Silently drop oldest logs when cap exceeded
        while (entries.size() > MAX_ENTRIES) {
            entries.remove(0);
        }
    }

    public List<ObjectNode> getAll() {
        return new ArrayList<>(entries);
    }

    public List<ObjectNode> getByTraceId(String traceId) {
        return entries.stream()
                .filter(e -> traceId.equals(e.path("trace_id").asText()))
                .collect(Collectors.toList());
    }

    public void clear() {
        entries.clear();
    }

    // Convenience shorthands used by services
    // These automatically capture the calling class name
    public void info(String service, String traceId, String message) {
        append("INFO", service, traceId, null, message, null);
    }

    public void warn(String service, String traceId, String errorType, String message) {
        append("WARN", service, traceId, errorType, message, null);
    }

    public void error(String service, String traceId, String errorType, String message) {
        append("ERROR", service, traceId, errorType, message, null);
    }
}
