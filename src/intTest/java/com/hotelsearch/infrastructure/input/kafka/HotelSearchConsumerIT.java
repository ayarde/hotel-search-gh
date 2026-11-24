package com.hotelsearch.infrastructure.input.kafka;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.hotelsearch.KafkaTestContainer;
import com.hotelsearch.OracleTestContainer;
import com.hotelsearch.application.port.out.HotelSearchPublisher;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.InvalidHotelSearchException;
import com.hotelsearch.domain.SearchId;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Consumer de búsquedas con la aplicación completa contra Kafka y Oracle
 * reales: persistencia, confirmación del offset, redelivery, reintentos y
 * DLT.
 *
 * <p>El repositorio es un spy sobre el adaptador JDBC real. Solo se stubbea
 * para el searchId de cada test; el resto de las llamadas llega a Oracle.
 */
@Testcontainers
@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HotelSearchConsumerIT {

    @Container
    static final GenericContainer<?> ORACLE = OracleTestContainer.create();

    @Container
    static final KafkaContainer KAFKA = KafkaTestContainer.create();

    private static final String HOTEL_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final LocalDate CHECK_IN = LocalDate.of(2026, 11, 9);
    private static final LocalDate CHECK_OUT = LocalDate.of(2026, 11, 29);
    private static final List<Integer> AGES = List.of(29, 28, 4, 3);
    private static final Instant CREATED_AT = Instant.parse("2026-11-17T12:00:00.123456Z");
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10);
    private static final long NO_COMMITTED_OFFSET = -1L;
    // Tres reintentos a 1 s cada uno; el margen cubre la granularidad del reloj.
    private static final Duration MIN_RETRY_SPAN = Duration.ofMillis(2_900);
    // Con un solo reintento ya se sumaría 1 s; con los tres, al menos 3 s.
    private static final Duration MAX_DIRECT_DLT_DELAY = Duration.ofSeconds(3);
    // Un intento más que el camino finito: con tres reintentos iría al DLT.
    private static final int OUTAGE_ATTEMPTS = 4;
    // Back-off de la caída: 1 + 2 + 4 + 8 = 15 s, más margen para el arranque.
    private static final Duration OUTAGE_WAIT_TIMEOUT = Duration.ofSeconds(45);

    @Autowired
    private HotelSearchPublisher publisher;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @MockitoSpyBean
    private HotelSearchRepository repository;

    @Value("${hotel-search.kafka.searches-topic}")
    private String searchesTopic;

    @Value("${hotel-search.kafka.dead-letter-topic}")
    private String deadLetterTopic;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroup;

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        OracleTestContainer.registerDataSource(registry, ORACLE);
        KafkaTestContainer.registerBootstrapServers(registry, KAFKA);
    }

    @BeforeEach
    void prepare() {
        jdbcClient.sql("delete from hotel_search").update();
        // Sin partición asignada, los tiempos medidos incluirían el rebalanceo
        // inicial del grupo.
        await().atMost(WAIT_TIMEOUT).until(this::listenersAssigned);
    }

    /**
     * Verifica que la aplicación crea los dos topics al arrancar, antes de
     * que ningún mensaje falle o se publique.
     *
     * Condiciones:
     * - Primer test de la clase, contexto recién levantado
     *
     * Se espera:
     * - hotel_availability_searches y hotel_search_dlt existen
     * - Cada uno con una partición y replication factor 1
     */
    @Test
    @Order(1)
    void initialize_ApplicationStartup_CreatesBothTopicsWithOnePartitionAndOneReplica() throws Exception {
        // given
        List<String> topics = List.of(searchesTopic, deadLetterTopic);

        // when
        Map<String, TopicDescription> descriptions;
        try (Admin admin = admin()) {
            descriptions = admin.describeTopics(topics).allTopicNames()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }

        // then
        assertAll(topics.stream().map(topic -> (Executable) () -> {
            List<TopicPartitionInfo> partitions = descriptions.get(topic).partitions();
            assertAll(
                    () -> assertEquals(1, partitions.size(), topic + " debería tener una partición"),
                    () -> assertEquals(1, partitions.getFirst().replicas().size(),
                            topic + " debería tener replication factor 1"));
        }));
    }

    /**
     * Verifica el camino feliz de punta a punta: lo que publica el producer
     * de la aplicación lo lee el listener y queda en Oracle. Fija la
     * compatibilidad del JSON entre ambos lados.
     *
     * Condiciones:
     * - Búsqueda publicada con el HotelSearchPublisher real
     *
     * Se espera:
     * - Una fila con los mismos valores del evento
     * - El grupo hotel-search confirma el offset hasta el final del topic
     */
    @Test
    void onMessage_SearchPublishedByProducer_PersistsRowAndCommitsOffset() throws Exception {
        // given
        SearchId searchId = SearchId.generate();
        HotelSearchEvent event = event(searchId);

        // when
        publisher.publish(event);

        // then
        long endOffset = endOffset();
        await().atMost(WAIT_TIMEOUT).until(() -> rowCount(searchId) == 1);
        await().atMost(WAIT_TIMEOUT).until(() -> committedOffset() >= endOffset);
        Map<String, Object> row = storedRow(searchId);
        assertAll(
                () -> assertEquals(HOTEL_ID, row.get("hotel_id"), "hotel_id inesperado"),
                () -> assertEquals("[29,28,4,3]", row.get("ages"), "ages debería conservar el orden"),
                () -> assertEquals("2026-11-09", row.get("check_in"), "check_in inesperado"),
                () -> assertEquals("2026-11-29", row.get("check_out"), "check_out inesperado"),
                () -> assertEquals("2026-11-17 12:00:00.123456", row.get("created_at"),
                        "created_at debería llegar con microsegundos"));
    }

    /**
     * Verifica que la redelivery de un evento ya persistido se descarta sin
     * error y sin una segunda fila.
     *
     * Condiciones:
     * - El mismo evento publicado dos veces
     *
     * Se espera:
     * - Dos llamadas a save, sin reintentos
     * - Una sola fila
     * - Offset confirmado hasta el final del topic
     */
    @Test
    void onMessage_SameEventTwice_PersistsOneRowAndCommitsBoth() throws Exception {
        // given
        SearchId searchId = SearchId.generate();
        HotelSearchEvent event = event(searchId);

        // when
        publisher.publish(event);
        publisher.publish(event);

        // then
        long endOffset = endOffset();
        await().atMost(WAIT_TIMEOUT).until(() -> committedOffset() >= endOffset);
        verify(repository, times(2)).save(argThat(hasSearchId(searchId)));
        assertEquals(1, rowCount(searchId), "La redelivery no debería crear otra fila");
    }

    /**
     * Verifica el tratamiento de un error transitorio de Oracle que no es
     * una caída: cada intento hace rollback, se reintenta tres veces a 1 s y
     * el mensaje termina en el DLT. El offset no se confirma mientras el
     * registro está en proceso.
     *
     * Condiciones:
     * - save hace el INSERT real y después lanza CannotAcquireLockException,
     *   en todos los intentos
     * - Mensaje publicado como bytes JSON con key = searchId
     *
     * Se espera:
     * - Cuatro intentos, separados en total por al menos 3 s
     * - Cada INSERT real encuentra la tabla sin la fila (rollback previo)
     * - Ninguna fila al final
     * - En cada intento, ya hecho el INSERT y con la transacción abierta, el
     *   offset confirmado no supera al registro; una confirmación anticipada
     *   (por ejemplo, ack al fallar el primer intento) se vería en los
     *   reintentos
     * - Un registro en el DLT con la misma key y los mismos bytes, y la
     *   causa CannotAcquireLockException en los headers
     * - Después del DLT, offset confirmado más allá del registro
     */
    @Test
    void onMessage_OracleFailsOnEveryAttempt_RollsBackRetriesAndSendsToDlt() throws Exception {
        // given
        SearchId searchId = SearchId.generate();
        byte[] payload = json(searchId, "[29,28,4,3]");
        List<Long> failureNanos = new CopyOnWriteArrayList<>();
        List<Object> realInsertResults = new CopyOnWriteArrayList<>();
        List<Long> committedDuringAttempts = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            realInsertResults.add(invocation.callRealMethod());
            committedDuringAttempts.add(committedOffset());
            failureNanos.add(System.nanoTime());
            throw new CannotAcquireLockException("Bloqueo no disponible (simulado)");
        }).when(repository).save(argThat(hasSearchId(searchId)));

        // when
        RecordMetadata published = send(searchId, payload);

        // then
        ConsumerRecord<String, byte[]> deadLetter = deadLetter(searchId);
        await().atMost(WAIT_TIMEOUT).until(() -> committedOffset() > published.offset());
        Duration retrySpan = Duration.ofNanos(failureNanos.getLast() - failureNanos.getFirst());
        assertAll(
                () -> assertEquals(4, failureNanos.size(), "Deberían ser un intento y tres reintentos"),
                () -> assertTrue(retrySpan.compareTo(MIN_RETRY_SPAN) >= 0,
                        "Los reintentos deberían esperar 1 s cada uno, el total fue " + retrySpan),
                () -> assertEquals(List.of(true, true, true, true), realInsertResults,
                        "Cada intento debería insertar sobre una tabla sin la fila del intento anterior"),
                () -> assertEquals(0, rowCount(searchId), "El rollback no debería dejar la fila"),
                () -> assertEquals(4, committedDuringAttempts.size(),
                        "Debería leerse el offset en cada intento"),
                () -> assertTrue(committedDuringAttempts.stream().allMatch(offset -> offset <= published.offset()),
                        "El offset no debería confirmarse mientras la transacción Oracle sigue abierta: "
                                + committedDuringAttempts),
                () -> assertArrayEquals(payload, deadLetter.value(), "El DLT debería recibir los bytes originales"),
                () -> assertEquals(CannotAcquireLockException.class.getName(), causeFqcn(deadLetter),
                        "El DLT debería informar la causa original"));
    }

    /**
     * Verifica que una caída de Oracle más larga que los tres reintentos del
     * camino finito no manda la búsqueda al DLT: se sigue reintentando y,
     * cuando Oracle vuelve, la fila se persiste.
     *
     * Condiciones:
     * - save lanza CannotGetJdbcConnectionException en los cuatro primeros
     *   intentos, sin llegar a Oracle, y hace el INSERT real en el quinto
     * - Mensaje publicado como bytes JSON con key = searchId
     *
     * Se espera:
     * - Cinco intentos y una fila con los valores del mensaje
     * - En cada intento fallido el offset confirmado no supera al registro
     * - Después de persistir, offset confirmado más allá del registro
     * - Nada en el DLT con esa key
     */
    @Test
    void onMessage_OracleUnavailableLongerThanRetries_PersistsWhenOracleReturns() throws Exception {
        // given
        SearchId searchId = SearchId.generate();
        byte[] payload = json(searchId, "[29,28,4,3]");
        AtomicInteger attempts = new AtomicInteger();
        List<Long> committedDuringOutage = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            if (attempts.incrementAndGet() > OUTAGE_ATTEMPTS) {
                return invocation.callRealMethod();
            }
            committedDuringOutage.add(committedOffset());
            throw new CannotGetJdbcConnectionException("Oracle no disponible (simulado)");
        }).when(repository).save(argThat(hasSearchId(searchId)));

        // when
        RecordMetadata published = send(searchId, payload);

        // then
        await().atMost(OUTAGE_WAIT_TIMEOUT).until(() -> rowCount(searchId) == 1);
        await().atMost(WAIT_TIMEOUT).until(() -> committedOffset() > published.offset());
        Map<String, Object> row = storedRow(searchId);
        List<ConsumerRecord<String, byte[]>> deadLetters = deadLetters(searchId);
        assertAll(
                () -> assertEquals(OUTAGE_ATTEMPTS + 1, attempts.get(),
                        "Deberían ser cuatro intentos fallidos y uno exitoso"),
                () -> assertEquals(OUTAGE_ATTEMPTS, committedDuringOutage.size(),
                        "Debería leerse el offset en cada intento fallido"),
                () -> assertTrue(committedDuringOutage.stream().allMatch(offset -> offset <= published.offset()),
                        "El offset no debería confirmarse mientras Oracle no está disponible: "
                                + committedDuringOutage),
                () -> assertEquals(HOTEL_ID, row.get("hotel_id"), "hotel_id inesperado"),
                () -> assertEquals("[29,28,4,3]", row.get("ages"), "ages debería conservar el orden"),
                () -> assertEquals("2026-11-09", row.get("check_in"), "check_in inesperado"),
                () -> assertEquals("2026-11-29", row.get("check_out"), "check_out inesperado"),
                () -> assertEquals("2026-11-17 12:00:00.000000", row.get("created_at"), "created_at inesperado"),
                () -> assertTrue(deadLetters.isEmpty(), "Una caída de Oracle no debería llegar al DLT"));
    }

    /**
     * Verifica que los errores permanentes van al DLT en el primer intento,
     * sin pasar por Oracle.
     *
     * Condiciones:
     * - Un JSON legible con una edad negativa
     * - Un payload que no es JSON válido
     *
     * Se espera:
     * - Cada uno en el DLT con su key y los mismos bytes
     * - Causa InvalidHotelSearchException e InvalidHotelSearchMessageException
     * - Menos de 3 s entre la publicación y el envío al DLT, según los
     *   timestamps de ambos registros: un solo reintento sumaría 1 s
     * - save nunca invocado
     */
    @Test
    void onMessage_PermanentError_SendsOriginalBytesToDltWithoutRetries() throws Exception {
        // given
        SearchId invalidAgesId = SearchId.generate();
        SearchId unreadableId = SearchId.generate();
        byte[] invalidAges = json(invalidAgesId, "[29,-1]");
        byte[] unreadable = "{\"searchId\":".getBytes(StandardCharsets.UTF_8);

        // when
        RecordMetadata invalidAgesSent = send(invalidAgesId, invalidAges);
        RecordMetadata unreadableSent = send(unreadableId, unreadable);

        // then
        ConsumerRecord<String, byte[]> invalidAgesDlt = deadLetter(invalidAgesId);
        ConsumerRecord<String, byte[]> unreadableDlt = deadLetter(unreadableId);
        assertAll(
                () -> assertArrayEquals(invalidAges, invalidAgesDlt.value(),
                        "El DLT debería recibir los bytes originales del JSON inválido"),
                () -> assertArrayEquals(unreadable, unreadableDlt.value(),
                        "El DLT debería recibir los bytes originales del payload ilegible"),
                () -> assertEquals(InvalidHotelSearchException.class.getName(), causeFqcn(invalidAgesDlt),
                        "Causa inesperada para la edad negativa"),
                () -> assertEquals(InvalidHotelSearchMessageException.class.getName(), causeFqcn(unreadableDlt),
                        "Causa inesperada para el payload ilegible"),
                () -> assertDirectToDlt(invalidAgesSent, invalidAgesDlt),
                () -> assertDirectToDlt(unreadableSent, unreadableDlt),
                () -> verify(repository, never()).save(any()));
    }

    /**
     * Verifica que la aplicación no consume el DLT: su reprocesamiento es
     * manual.
     *
     * Condiciones:
     * - Contexto levantado con todos los listeners registrados
     *
     * Se espera:
     * - Al menos un container y todos escuchando solo
     *   hotel_availability_searches
     */
    @Test
    void getListenerContainers_ApplicationStartup_NoContainerListensToDlt() {
        // given
        Set<String> expectedTopics = Set.of(searchesTopic);

        // when
        Collection<MessageListenerContainer> containers = listenerRegistry.getListenerContainers();

        // then
        Set<String> topics = containers.stream()
                .flatMap(container -> List.of(container.getContainerProperties().getTopics()).stream())
                .collect(Collectors.toSet());
        assertAll(
                () -> assertFalse(containers.isEmpty(), "Debería existir el listener de búsquedas"),
                () -> assertEquals(expectedTopics, topics, "Ningún listener debería consumir el DLT"));
    }

    // <editor-fold desc="Helpers">
    private static HotelSearchEvent event(SearchId searchId) {
        return new HotelSearchEvent(searchId, HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, AGES), CREATED_AT);
    }

    /** Mismo formato que publica el producer de la aplicación. */
    private static byte[] json(SearchId searchId, String ages) {
        return """
                {"searchId":"%s","hotelId":"%s","checkIn":"2026-11-09","checkOut":"2026-11-29",\
                "ages":%s,"createdAt":"2026-11-17T12:00:00Z"}"""
                .formatted(searchId.value(), HOTEL_ID, ages)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static ArgumentMatcher<HotelSearchEvent> hasSearchId(SearchId searchId) {
        return event -> event != null && searchId.equals(event.searchId());
    }

    private static String causeFqcn(ConsumerRecord<String, byte[]> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void assertDirectToDlt(RecordMetadata original, ConsumerRecord<String, byte[]> deadLetter) {
        // El recoverer no copia el timestamp: el del registro DLT es el de su envío.
        Duration delay = Duration.ofMillis(deadLetter.timestamp() - original.timestamp());
        assertTrue(delay.compareTo(MAX_DIRECT_DLT_DELAY) < 0,
                "Un error permanente no debería reintentarse, llegó al DLT en " + delay);
    }

    private boolean listenersAssigned() {
        return listenerRegistry.getListenerContainers().stream().allMatch(container -> {
            Collection<TopicPartition> assigned = container.getAssignedPartitions();
            return assigned != null && !assigned.isEmpty();
        });
    }

    private TopicPartition searchesPartition() {
        return new TopicPartition(searchesTopic, 0);
    }

    private RecordMetadata send(SearchId searchId, byte[] payload) throws Exception {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(config)) {
            return producer.send(new ProducerRecord<>(searchesTopic, searchId.value().toString(), payload))
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private ConsumerRecord<String, byte[]> deadLetter(SearchId searchId) {
        String key = searchId.value().toString();
        try (KafkaConsumer<String, byte[]> consumer = deadLetterConsumer()) {
            Instant deadline = Instant.now().plus(WAIT_TIMEOUT);
            while (Instant.now().isBefore(deadline)) {
                List<ConsumerRecord<String, byte[]>> found =
                        StreamSupport.stream(consumer.poll(Duration.ofMillis(500)).spliterator(), false)
                                .filter(record -> key.equals(record.key()))
                                .toList();
                if (!found.isEmpty()) {
                    assertEquals(1, found.size(), "Debería haber un solo registro en el DLT para " + key);
                    return found.getFirst();
                }
            }
            throw new AssertionError("No llegó al DLT el registro con key " + key);
        }
    }

    /** Lee el DLT completo, desde el principio hasta el final actual. */
    private List<ConsumerRecord<String, byte[]>> deadLetters(SearchId searchId) {
        String key = searchId.value().toString();
        List<ConsumerRecord<String, byte[]>> found = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = deadLetterConsumer()) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment(), CLIENT_TIMEOUT);
            Instant deadline = Instant.now().plus(WAIT_TIMEOUT);
            while (endOffsets.entrySet().stream()
                    .anyMatch(end -> consumer.position(end.getKey(), CLIENT_TIMEOUT) < end.getValue())) {
                if (Instant.now().isAfter(deadline)) {
                    throw new AssertionError("No se pudo leer el DLT hasta el final");
                }
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    if (key.equals(record.key())) {
                        found.add(record);
                    }
                });
            }
        }
        return found;
    }

    /** Consumer sin grupo compartido, posicionado al principio del DLT. */
    private KafkaConsumer<String, byte[]> deadLetterConsumer() {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(config);
        List<TopicPartition> partitions = consumer.partitionsFor(deadLetterTopic).stream()
                .map(info -> new TopicPartition(deadLetterTopic, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        return consumer;
    }

    private Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    private long committedOffset() throws Exception {
        try (Admin admin = admin()) {
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                    .get(searchesPartition());
            return committed == null ? NO_COMMITTED_OFFSET : committed.offset();
        }
    }

    private long endOffset() throws Exception {
        try (Admin admin = admin()) {
            return admin.listOffsets(Map.of(searchesPartition(), OffsetSpec.latest()))
                    .partitionResult(searchesPartition())
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                    .offset();
        }
    }

    private int rowCount(SearchId searchId) {
        return jdbcClient.sql("select count(*) from hotel_search where search_id = :searchId")
                .param("searchId", searchId.value().toString())
                .query(Integer.class)
                .single();
    }

    private Map<String, Object> storedRow(SearchId searchId) {
        return jdbcClient.sql("""
                        select hotel_id,
                               ages,
                               to_char(check_in, 'yyyy-mm-dd') as check_in,
                               to_char(check_out, 'yyyy-mm-dd') as check_out,
                               to_char(created_at, 'yyyy-mm-dd hh24:mi:ss.ff6') as created_at
                          from hotel_search
                         where search_id = :searchId
                        """)
                .param("searchId", searchId.value().toString())
                .query()
                .singleRow();
    }
    // </editor-fold>
}
