# evi-login-processor

Developed based on requirements: docs/requirements/requirements.txt

## Project Description

The **evi-login-processor** is a **Spring Boot reactive microservice** built with **Spring WebFlux** and **Reactor Kafka
** to process customer login events in real-time. The service ensures **exactly-once processing** for each login event,
avoiding duplicate database saves or duplicate event publishing.

High-level architecture diagram:

[evi-login-tracker.v0.excalidraw](docs/architecture/evi-login-tracker.v0.excalidraw)

It handles the following workflow:

---

## Kafka Workflow

### 1. `customer-login` Topic

* This topic contains incoming login events with the following format:

```
- customerId (UUID)
- username
- client (web/android/ios)
- timestamp
- messageId (UUID)
- customerIp
```

### 2. Consumer Group 1: `consumer-customer-login`

* Reads messages from the `customer-login` topic.
* Performs a **REST call to the customer tracking service** for each event using **WebClient**.
* Retries the REST call **up to 3 times** in case of failure.
* Publishes the enriched result to the topic `customer-login-result`.

### 3. Consumer Group 2: `consumer-customer-login-result`

* Reads messages from the `customer-login-result` topic.
* Saves each enriched login event into the database with the following format:

```
- customerId (UUID)
- username
- client (web/android/ios)
- timestamp
- messageId (UUID)
- customerIp
- requestResult ('successful' or 'unsuccessful')
```

* Only **after a successful save** in the database, it publishes the enriched message to the final topic:
  `login-tracking-result`.

---

## Exactly-Once Processing

The service guarantees **no duplicate processing** by:

1. Consuming each Kafka message **exactly once** using **Reactor Kafka transactional producers**.
2. Persisting each login event **exactly once** in the database.
3. Publishing the tracking result message **exactly once** after a successful database save.

This ensures reliable end-to-end processing with **exactly-once semantics**.

---

## Technology Stack

* **Spring Boot** (WebFlux, Reactive)
* **Reactor Kafka** for reactive Kafka integration
* **Spring Data R2DBC** for reactive database access
* **WebClient** for REST API calls
* **Gradle** for build automation
* **JUnit 5 + WireMock + Testcontainers** for unit and integration tests
* **Jacoco** for code coverage (≥ 90%)

---

## Kafka Topics Overview

| Topic Name              | Description                                                |
|-------------------------|------------------------------------------------------------|
| `customer-login`        | Incoming login events                                      |
| `customer-login-result` | Events enriched with REST call result                      |
| `login-tracking-result` | Events saved successfully in DB and finalized for tracking |

---

## Processing Flow Diagram

```
customer-login topic
       │
       ▼
consumer-customer-login
  - REST call with 3 retries
       │
       ▼
customer-login-result topic
       │
       ▼
consumer-customer-login-result
  - Save to DB
  - If successful:
       ▼
login-tracking-result topic
```

---

## How to Run

1. Configure Kafka brokers in `application.yml`
2. Configure database connection (R2DBC/PostgreSQL recommended)
3. Build the project with Gradle:

```bash
./gradlew build
```

4. Run the Spring Boot application:

```bash
./gradlew bootRun
```

5. Ensure Kafka topics (`customer-login`, `customer-login-result`, `login-tracking-result`) exist or are auto-created by
   Kafka.

---

## Testing

* Integration tests use **Testcontainers Kafka** and **WireMock** for simulating REST calls.
* Unit tests cover **service logic** with at least 90% code coverage (Jacoco reports).
* Reactive flows and transactional guarantees are fully tested end-to-end.

---

## Notes

* The service is **fully reactive** and handles backpressure naturally.
* REST failures are retried **3 times**, and failures after that are marked as `'unsuccessful'`.
* Database save is **transactional** to avoid duplicates.
* Uses Reactor Kafka transactions to guarantee exactly-once delivery across Kafka and the database.
* Could be improved by adjusting to the following
  architecture: [evi-login-tracker.v1.excalidraw](docs/architecture/evi-login-tracker.v1.excalidraw) 
