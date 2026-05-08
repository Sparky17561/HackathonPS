# HackathonPS: Production-Like Unstable Backend

An intentionally unstable Java Spring Boot backend system designed for AI debugging hackathons. This system provides realistic business APIs with natural production-like bugs, enabling participants to build AI systems that analyze logs, detect failures, identify root causes, and suggest fixes.

---

## Project Overview

This is a production-like unstable backend environment that simulates real-world system failures. The system exposes realistic e-commerce APIs (orders, payments, inventory) with intentional bugs that occur naturally through API usage patterns. All failures are documented and generate structured JSON logs, making the system an ideal debugging target for AI-powered observability and root cause analysis (RCA) systems.

**Key Characteristics:**
- Realistic business APIs mimicking e-commerce operations
- Intentional natural bugs (no artificial test flags)
- Structured JSON logging with trace correlation
- Probabilistic failure injection (25% intermittent failure rate)
- Async processing with trace context propagation issues
- No authentication (designed for hackathon accessibility)

---

## Features

- **Order Management**: Create, retrieve, and cancel orders with state transitions
- **Payment Processing**: Process payments and refunds with gateway simulation
- **Inventory Management**: Real-time stock tracking with reservation logic
- **Structured Logging**: JSON-formatted logs with trace IDs and error classification
- **Trace Correlation**: Request-level trace propagation across service boundaries
- **Async Processing**: Background jobs for audits and confirmations
- **Intermittent Failures**: Probabilistic dependency failures (25% rate)
- **Retry/Idempotency Issues**: Duplicate operations on retry scenarios
- **Payment Gateway Simulation**: Timeout and failure scenarios
- **Log Query API**: Programmatic access to structured logs

---

## Tech Stack

- **Java 17**: Core runtime
- **Spring Boot 3.2.5**: Application framework
- **Maven**: Build and dependency management
- **Logstash Logback Encoder**: Structured JSON logging
- **Spring Actuator**: Health and metrics endpoints
- **Docker**: Containerized deployment
- **Render**: Cloud deployment platform

---

## Public Deployment

- **Live URL**: https://hackathonps.onrender.com
- **Status**: Deployed via Docker runtime on Render
- **Availability**: Public API access (no authentication required)

---

## GitHub Repository

- **Repository**: https://github.com/Sparky17561/HackathonPS

---

## API Documentation

### Health Check

**Endpoint**: `GET /health`  
**Method**: GET  
**Purpose**: System health status check

**Sample Response**:
```json
{
  "status": "UP",
  "timestamp": "2024-05-08T00:00:00Z",
  "services": {
    "order": "STABLE",
    "payment": "DEGRADED",
    "inventory": "STABLE"
  }
}
```

---

### Inventory

**Endpoint**: `GET /inventory`  
**Method**: GET  
**Purpose**: Retrieve current inventory state for all products

**Sample Response**:
```json
{
  "PROD-001": {
    "productId": "PROD-001",
    "name": "Wireless Headphones",
    "stock": 45,
    "reservedStock": 5,
    "price": 79.99
  }
}
```

---

### Logs

**Endpoint**: `GET /logs` or `GET /logs?trace_id={trace_id}`  
**Method**: GET  
**Purpose**: Fetch structured JSON logs, optionally filtered by trace ID

**Sample Response**:
```json
[
  {
    "timestamp": "2024-05-08T00:00:00.123Z",
    "trace_id": "a1b2c3d4e5f6g7h8",
    "service": "PaymentService",
    "error_type": "GATEWAY_TIMEOUT",
    "message": "Payment gateway did not respond within SLA for orderId=xxx",
    "level": "WARN"
  }
]
```

---

### Order

**Endpoint**: `POST /order`  
**Method**: POST  
**Purpose**: Create a new order with specified items

**Sample Request**:
```bash
curl -X POST https://hackathonps.onrender.com/order \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-123",
    "items": [
      {"productId": "PROD-001", "quantity": 2, "unitPrice": 79.99}
    ]
  }'
```

**Sample Response**:
```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "RESERVED",
  "trace_id": "a1b2c3d4e5f6g7h8"
}
```

---

### Payment

**Endpoint**: `POST /pay`  
**Method**: POST  
**Purpose**: Process payment for an existing order

**Sample Request**:
```bash
curl -X POST https://hackathonps.onrender.com/pay \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "user-123",
    "amount": 159.98
  }'
```

**Sample Response**:
```json
{
  "paymentId": "660e8400-e29b-41d4-a716-446655440001",
  "status": "SUCCESS",
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "trace_id": "a1b2c3d4e5f6g7h8"
}
```

---

## Example Failure Scenarios

### Gateway Timeout (Duplicate Payment)

**Scenario**: Payment gateway timeout (25% probability) causes client retry, creating duplicate payment records.

**Log Pattern**:
```json
{"error_type": "GATEWAY_TIMEOUT", "message": "Payment gateway did not respond within SLA"}
{"error_type": "DUPLICATE_PAYMENT_DETECTED", "message": "Multiple payments recorded for orderId=xxx count=2"}
```

**AI Detection**: Correlate retry patterns with duplicate payment records, identify missing idempotency.

---

### Order Update Failure (Partial Write)

**Scenario**: Payment record created successfully, but order status update fails (10% probability), leaving orphaned payment.

**Log Pattern**:
```json
{"error_type": "ORDER_UPDATE_FAILURE", "message": "Payment=xxx recorded but order=yyy not updated to PAID"}
```

**AI Detection**: Identify partial writes, detect orphaned records, suggest transaction boundaries.

---

### Async Trace Context Loss

**Scenario**: Background async jobs lose MDC context, generating logs without trace correlation.

**Log Pattern**:
```json
{"trace_id": "unknown", "message": "Async audit: verifying deduction integrity"}
{"trace_id": "ASYNC-ORPHAN", "error_type": "CONFIRM_NOTIFICATION_FAILED", "message": "..."}
```

**AI Detection**: Identify orphaned logs, spot broken trace chains, detect async propagation issues.

---

### Race Condition (Inventory Overselling)

**Scenario**: Concurrent order requests bypass stock check due to non-atomic check-then-act pattern.

**Log Pattern**:
```json
{"error_type": "NEGATIVE_STOCK", "message": "Stock is now negative for PROD-001 value=-5"}
```

**AI Detection**: Correlate timing patterns, identify check-then-act anti-pattern, detect race conditions.

---

### Inconsistent Order State

**Scenario**: Order persisted before inventory reservation, intermittent failure leaves order in CREATED state without reservation.

**Log Pattern**:
```json
{"error_type": "INCONSISTENT_STATE", "message": "Order left in CREATED state despite reservation failure"}
```

**AI Detection**: Identify state inconsistencies, detect transaction ordering violations.

---

## Structured Logging

The system uses Logstash Logback Encoder for structured JSON logging. Each log entry contains:

**Log Fields:**
- `timestamp`: ISO-8601 formatted timestamp
- `trace_id`: 16-character request correlation identifier
- `service`: Service name (OrderService, PaymentService, InventoryService)
- `error_type`: Classified error type for AI analysis
- `message`: Human-readable log message with contextual data
- `level`: Log level (INFO, WARN, ERROR)
- `class`: Fully qualified class name
- `thread`: Thread name

**Example Log Entry:**
```json
{
  "timestamp": "2024-05-08T00:00:00.123Z",
  "trace_id": "a1b2c3d4e5f6g7h8",
  "service": "PaymentService",
  "class": "com.hacksys.backend.service.PaymentService",
  "thread": "http-nio-8080-exec-1",
  "level": "WARN",
  "error_type": "GATEWAY_TIMEOUT",
  "message": "Payment gateway did not respond within SLA for orderId=550e8400-e29b-41d4-a716-446655440000",
  "app": "hacksys-backend"
}
```

---

## Hackathon Purpose

This backend is intentionally unstable and acts as a realistic debugging target for AI-powered observability and root cause analysis systems. The system is designed to:

- Provide realistic business APIs that participants can interact with
- Generate diverse failure patterns through natural API usage
- Produce structured logs suitable for AI analysis and pattern detection
- Simulate production-like bugs without artificial test flags
- Challenge AI systems to correlate logs across service boundaries
- Test AI capabilities in identifying root causes and suggesting fixes

**This is NOT a production commerce system.** This is a hackathon debugging environment designed for educational and competitive purposes.

---

## Running Locally

**Prerequisites:**
- Java 17 installed
- Maven installed

**Build:**
```bash
./mvnw clean install
```

**Run:**
```bash
./mvnw spring-boot:run
```

**Access APIs:**
- Application: http://localhost:8080
- Health check: http://localhost:8080/health
- Logs: http://localhost:8080/logs

---

## Docker Deployment

The project includes a multi-stage Dockerfile for containerized deployment.

**Build Image:**
```bash
docker build -t hackathonps .
```

**Run Container:**
```bash
docker run -p 8080:8080 hackathonps
```

**Dockerfile Details:**
- Build stage: Maven 3.9.6 with Java 17 for compilation
- Runtime stage: Eclipse Temurin JRE 17 Alpine for minimal image size
- Port: 8080 exposed
- Entry point: `java -jar app.jar`

---

## Important Note

**Failures are intentional.** This system contains documented bugs designed to simulate production incidents. The instability is probabilistic (25% intermittent failure rate) and occurs naturally through API usage patterns.

**This is NOT a production commerce system.** Do not use this code as a reference for building real e-commerce systems. The intentional bugs (missing idempotency, race conditions, partial writes, state machine violations) are design choices for hackathon purposes.

**This IS a hackathon debugging environment.** The system is optimized for AI log analysis training, not for reliability, security, or scalability.
