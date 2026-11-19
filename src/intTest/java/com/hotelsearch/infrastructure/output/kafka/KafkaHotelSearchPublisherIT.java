package com.hotelsearch.infrastructure.output.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hotelsearch.KafkaTestContainer;
import com.hotelsearch.application.port.out.HotelSearchPublicationException;
import com.hotelsearch.application.port.out.HotelSearchPublisher;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import com.hotelsearch.infrastructure.config.KafkaProducerConfiguration;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publisher de búsquedas contra un broker Kafka real, con la configuración de
 * producer de {@code application.yml}. Solo levanta la autoconfiguración de
 * Kafka y el adaptador: la publicación no necesita Oracle.
 */
@Testcontainers
@SpringBootTest(classes = KafkaProducerConfiguration.class)
@ImportAutoConfiguration(KafkaAutoConfiguration.class)
class KafkaHotelSearchPublisherIT {

    @Container
    static final KafkaContainer KAFKA = KafkaTestContainer.create();

    private static final String HOTEL_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final LocalDate CHECK_IN = LocalDate.of(2026, 11, 9);
    private static final LocalDate CHECK_OUT = LocalDate.of(2026, 11, 29);
    private static final List<Integer> AGES = List.of(29, 28, 4, 3);
    private static final Instant CREATED_AT = Instant.parse("2026-11-17T12:00:00.123456Z");
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);

    @Autowired
    private HotelSearchPublisher publisher;

    @Autowired
    private DefaultKafkaProducerFactory<String, HotelSearchMessage> producerFactory;

    @Value("${hotel-search.kafka.searches-topic}")
    private String topic;

    private ProducerFactory<String, HotelSearchMessage> unreachableProducerFactory;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        KafkaTestContainer.registerBootstrapServers(registry, KAFKA);
    }

    @AfterEach
    void closeUnreachableProducer() {
        if (unreachableProducerFactory != null) {
            unreachableProducerFactory.reset();
        }
    }

    /**
     * Verifica que, cuando publish retorna, el broker ya tiene el mensaje con
     * el searchId como key y el evento serializado como JSON.
     *
     * Condiciones:
     * - Broker disponible
     * - Topic hotel_availability_searches
     *
     * Se espera:
     * - El registro se puede leer del topic apenas publish retorna
     * - key = searchId
     * - Payload JSON con fechas ISO-8601, edades en orden y createdAt
     * - Sin header de tipo Java
     */
    @Test
    void publish_BrokerAvailable_StoresRecordKeyedBySearchId() {
        // given
        SearchId searchId = SearchId.generate();
        HotelSearchEvent event = new HotelSearchEvent(searchId, HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, AGES),
                CREATED_AT);

        // when
        publisher.publish(event);

        // then
        ConsumerRecord<String, String> stored = storedRecord(searchId.value().toString())
                .orElseThrow(() -> new AssertionError("El registro debería estar en el topic al retornar publish"));
        JsonNode expectedPayload = json("""
                {
                  "searchId": "%s",
                  "hotelId": "%s",
                  "checkIn": "2026-11-09",
                  "checkOut": "2026-11-29",
                  "ages": [29, 28, 4, 3],
                  "createdAt": "2026-11-17T12:00:00.123456Z"
                }
                """.formatted(searchId.value(), HOTEL_ID));
        assertAll(
                () -> assertEquals(searchId.value().toString(), stored.key(), "La key debería ser el searchId"),
                () -> assertEquals(expectedPayload, json(stored.value()), "Payload JSON inesperado"),
                () -> assertNull(stored.headers().lastHeader("__TypeId__"),
                        "El mensaje no debería llevar el nombre de la clase Java"));
    }

    /**
     * Verifica que el producer que usa el publisher tiene la configuración
     * acordada de ACK, idempotencia y timeouts.
     *
     * Condiciones:
     * - Propiedades de application.yml
     *
     * Se espera:
     * - acks=all y enable.idempotence=true
     * - delivery.timeout.ms=10000, request.timeout.ms=3000 y max.block.ms=5000
     */
    @Test
    void getConfigurationProperties_ApplicationConfig_RequiresFullAckIdempotenceAndTimeouts() {
        // given
        Map<String, Object> expected = Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000",
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000",
                ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");

        // when
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        // then
        assertAll(expected.entrySet().stream().map(entry -> (Executable) () -> assertEquals(entry.getValue(),
                String.valueOf(config.get(entry.getKey())), "Valor inesperado para " + entry.getKey())));
    }

    /**
     * Verifica que, sin broker alcanzable, publish falla con el error del
     * puerto dentro del tiempo configurado en lugar de quedar bloqueado.
     *
     * Condiciones:
     * - Misma configuración de producer, bootstrap a un puerto local sin broker
     *
     * Se espera:
     * - HotelSearchPublicationException
     * - Una TimeoutException de Kafka en la cadena de causas
     * - El fallo llega antes de delivery.timeout.ms (10 s)
     */
    @Test
    void publish_BrokerUnreachable_ThrowsPublicationExceptionWithinTimeout() throws IOException {
        // given
        unreachableProducerFactory = producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + freeLocalPort()));
        HotelSearchPublisher unreachablePublisher =
                new KafkaHotelSearchPublisher(new KafkaTemplate<>(unreachableProducerFactory), topic);
        HotelSearchEvent event = new HotelSearchEvent(SearchId.generate(),
                HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, AGES), CREATED_AT);
        Instant start = Instant.now();

        // when
        Executable action = () -> unreachablePublisher.publish(event);

        // then
        HotelSearchPublicationException thrown = assertThrows(HotelSearchPublicationException.class, action,
                "Sin broker la publicación debería fallar");
        Duration elapsed = Duration.between(start, Instant.now());
        assertAll(
                () -> assertTrue(causes(thrown).anyMatch(TimeoutException.class::isInstance),
                        "La causa debería ser un timeout del producer"),
                () -> assertTrue(elapsed.compareTo(Duration.ofSeconds(10)) < 0,
                        "El fallo debería llegar antes de 10 s, tardó " + elapsed));
    }

    // <editor-fold desc="Helpers">
    private Optional<ConsumerRecord<String, String>> storedRecord(String key) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Instant deadline = Instant.now().plus(POLL_TIMEOUT);
            while (Instant.now().isBefore(deadline)) {
                Optional<ConsumerRecord<String, String>> found =
                        StreamSupport.stream(consumer.poll(Duration.ofMillis(500)).spliterator(), false)
                                .filter(record -> key.equals(record.key()))
                                .findFirst();
                if (found.isPresent()) {
                    return found;
                }
            }
            return Optional.empty();
        }
    }

    private static JsonNode json(String content) {
        return JsonMapper.shared().readTree(content);
    }

    private static Stream<Throwable> causes(Throwable thrown) {
        return Stream.iterate(thrown.getCause(), cause -> cause != null, Throwable::getCause);
    }

    private static int freeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
    // </editor-fold>
}
