package com.evi.login.processor;

import com.evi.login.processor.util.TestBeansConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * EviLoginTrackerApplicationTest
 */
@Import({TestBeansConfig.class})
@SpringBootTest(properties = {"spring.main.allow-bean-definition-overriding=true",
        "spring.kafka.consumer.auto-startup=false"})
@EnableAutoConfiguration(exclude = {
        R2dbcAutoConfiguration.class,
        KafkaAutoConfiguration.class,
        DataSourceAutoConfiguration.class})
class EviLoginTrackerApplicationTest {

    @Test
    void contextLoads() {
        // This will pass if the Spring context starts successfully
    }

}
