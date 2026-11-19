package com.hotelsearch;

import java.time.Duration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Apache Kafka 4.4.0 en modo KRaft, un broker, para los integration tests.
 *
 * <p>Es la misma imagen oficial que usa Docker Compose. Cada clase de test
 * declara su propio contenedor con {@link #create()}.
 */
public final class KafkaTestContainer {

    private static final DockerImageName IMAGE = DockerImageName.parse("apache/kafka:4.4.0");

    private KafkaTestContainer() {
    }

    public static KafkaContainer create() {
        return new KafkaContainer(IMAGE)
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
                .withStartupTimeout(Duration.ofMinutes(2));
    }

    public static void registerBootstrapServers(DynamicPropertyRegistry registry, KafkaContainer kafka) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
