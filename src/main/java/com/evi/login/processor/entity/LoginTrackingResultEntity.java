package com.evi.login.processor.entity;

import com.evi.login.processor.model.RequestResult;
import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jdk.jfr.Description;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login-result, to topic login-tracking-result")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "login_tracking_result",
        schema = "login_tracker_db")
public class LoginTrackingResultEntity {

    @Id
    @Column(name = "message_id", nullable = false)
    private UUID messageId;

    private UUID customerId;
    private String username;
    private String client;
    private Instant timestamp;
    private String customerIp;
    private RequestResult requestResult;
}
/*
 * # 📊 UUID vs Auto-increment (Long):
 *
 * ## ✅ UUID – Advantages
 * * **Globally unique**
 * → safe across distributed systems (microservices, Kafka)
 * * **DB-independent generation**
 * → IDs can be created in the application, no DB round-trip needed
 * * **Easy data merging**
 * → no conflicts when combining multiple databases
 * * **Secure / non-predictable**
 * → cannot be guessed (unlike `1, 2, 3...`)
 * ---
 * ## ⚠️ UUID – Disadvantages
 * * **Larger size**
 * → 16 bytes vs 8 bytes (`Long`)
 * * **Slower inserts**
 * → causes index fragmentation (especially random UUIDs)
 * * **Harder to read/debug**
 * → e.g. `550e8400-e29b-41d4-a716-446655440000`
 * ---
 * ## ⚡ Auto-increment (Long) – Advantages
 * * **High performance**
 * → sequential inserts are very efficient for indexes
 * * **Smaller storage**
 * → 8 bytes
 * * **Human-readable**
 * → e.g. `12345`
 * ---
 * ## ❌ Auto-increment (Long) – Disadvantages
 * * **Not globally unique**
 * → problematic in distributed systems
 * * **DB-dependent**
 * → requires insert to generate ID
 * * **Predictable (security risk)**
 * → easy to enumerate
 * ---
 * # 🧠 Practical Recommendation (most important)
 * 👉 **Best practice: combine both**
 * * `Long id` → primary key (performance)
 * * `UUID messageId` → uniqueness + idempotency
 * ---
 * # 🎯 Simple rule to remember
 * <p>
 * * **UUID** → distributed systems, Kafka, global uniqueness
 * * **Long** → database performance
 * * **Both** → real-world production systems ✅
 */