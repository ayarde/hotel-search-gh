package com.hotelsearch.infrastructure.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hotelsearch.domain.InvalidHotelSearchException;
import com.hotelsearch.infrastructure.input.kafka.InvalidHotelSearchMessageException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigurationTest {

    private static final String SEARCHES_TOPIC = "hotel_availability_searches";
    private static final String DLT = "hotel_search_dlt";
    private static final byte[] PAYLOAD = "{\"searchId\":\"x\"}".getBytes(UTF_8);
    // Suficientes para pasar varias veces el tope de 30 s: 1, 2, 4, 8, 16, 30...
    private static final int BACK_OFF_SAMPLES = 50;
    private static final long FIRST_BACK_OFF_MS = 1_000L;
    private static final long MAX_BACK_OFF_MS = 30_000L;
    // Más intentos que el camino finito (cuatro): ahí el último ya iría al DLT.
    private static final int INFRASTRUCTURE_ATTEMPTS = 6;

    private final KafkaConsumerConfiguration configuration = new KafkaConsumerConfiguration();

    @Mock
    private Consumer<?, ?> consumer;

    @Mock
    private MessageListenerContainer container;

    /**
     * Verifica que los errores permanentes van al DLT en el primer intento,
     * sin reintentos.
     *
     * Condiciones:
     * - Mensaje ilegible y evento con estructura inválida, envueltos como los
     *   entrega el container
     *
     * Se espera:
     * - handleOne recupera el registro en el primer intento
     * - El recoverer recibe cada registro una sola vez
     */
    @Test
    void hotelSearchErrorHandler_PermanentError_RecoversOnFirstAttempt() {
        // given
        List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
        DefaultErrorHandler errorHandler = configuration.hotelSearchErrorHandler(
                (record, exception) -> recovered.add(record));
        ConsumerRecord<String, byte[]> unreadable = record(1);
        ConsumerRecord<String, byte[]> invalid = record(2);

        // when
        boolean unreadableRecovered = errorHandler.handleOne(
                listenerFailure(new InvalidHotelSearchMessageException("JSON ilegible", null)),
                unreadable, consumer, container);
        boolean invalidRecovered = errorHandler.handleOne(
                listenerFailure(new InvalidHotelSearchException("ages es obligatorio")),
                invalid, consumer, container);

        // then
        assertAll(
                () -> assertTrue(unreadableRecovered, "Un mensaje ilegible debería ir al DLT sin reintentos"),
                () -> assertTrue(invalidRecovered, "Un evento inválido debería ir al DLT sin reintentos"),
                () -> assertEquals(List.of(unreadable, invalid), recovered,
                        "Cada registro debería recuperarse una sola vez"));
    }

    /**
     * Verifica que un error transitorio que no indica Oracle caído se
     * reintenta tres veces más antes de ir al DLT.
     *
     * Condiciones:
     * - Bloqueo que no se pudo adquirir (CannotAcquireLockException) en
     *   todos los intentos
     *
     * Se espera:
     * - Los tres primeros fallos no recuperan el registro
     * - El cuarto fallo lo envía al recoverer
     */
    @Test
    void hotelSearchErrorHandler_TransientError_RecoversAfterThreeRetries() {
        // given
        List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
        DefaultErrorHandler errorHandler = configuration.hotelSearchErrorHandler(
                (record, exception) -> recovered.add(record));
        ConsumerRecord<String, byte[]> record = record(3);
        Exception failure = listenerFailure(new CannotAcquireLockException("Bloqueo no disponible"));
        List<Boolean> attempts = new ArrayList<>();

        // when
        for (int attempt = 0; attempt < 4; attempt++) {
            attempts.add(errorHandler.handleOne(failure, record, consumer, container));
        }

        // then
        assertAll(
                () -> assertEquals(List.of(false, false, false, true), attempts,
                        "Debería recuperarse recién en el cuarto intento"),
                () -> assertEquals(List.of(record), recovered, "El registro debería ir una vez al DLT"));
    }

    /**
     * Verifica el back-off para Oracle no disponible: exponencial desde 1 s,
     * con tope de 30 s y sin fin.
     *
     * Condiciones:
     * - Cada uno de los cuatro tipos que indican Oracle caído, envuelto como
     *   lo entrega el container
     * - CannotCreateTransactionException anidada dos niveles debajo del
     *   wrapper del container
     *
     * Se espera:
     * - 1000 ms en el primer reintento y el doble en cada uno hasta 30000 ms
     * - Ninguno de los primeros 50 valores es STOP
     */
    @Test
    void backOffFor_InfrastructureUnavailable_RetriesForeverWithExponentialBackOff() {
        // given
        List<Exception> failures = List.of(
                listenerFailure(new CannotCreateTransactionException("Sin conexión para iniciar la transacción")),
                listenerFailure(new DataAccessResourceFailureException("Oracle no disponible")),
                listenerFailure(new TransientDataAccessResourceException("Conexión rechazada")),
                listenerFailure(new RecoverableDataAccessException("Conexión perdida")),
                listenerFailure(new IllegalStateException("Persistencia fallida",
                        new CannotCreateTransactionException("Sin conexión para iniciar la transacción"))));

        // when
        List<BackOff> backOffs = failures.stream().map(KafkaConsumerConfiguration::backOffFor).toList();

        // then
        assertAll(backOffs.stream().map(backOff -> (Executable) () -> {
            assertNotNull(backOff, "Oracle no disponible debería tener su propio back-off");
            List<Long> intervals = intervals(backOff);
            assertAll(
                    () -> assertEquals(List.of(FIRST_BACK_OFF_MS, 2_000L, 4_000L, 8_000L, 16_000L, MAX_BACK_OFF_MS),
                            intervals.subList(0, 6), "Debería empezar en 1 s y duplicar hasta el tope"),
                    () -> assertTrue(intervals.stream().allMatch(interval -> interval <= MAX_BACK_OFF_MS),
                            "Ningún intervalo debería superar 30 s: " + intervals),
                    () -> assertTrue(intervals.stream().noneMatch(interval -> interval == BackOffExecution.STOP),
                            "El back-off no debería terminar nunca: " + intervals));
        }));
    }

    /**
     * Verifica que solo Oracle caído recibe el back-off sin fin: el resto de
     * los errores sigue con el back-off por defecto del error handler.
     *
     * Condiciones:
     * - Bloqueo no disponible y error genérico (camino finito)
     * - Mensaje ilegible y evento inválido (permanentes)
     *
     * Se espera:
     * - backOffFor devuelve null en todos los casos
     */
    @Test
    void backOffFor_OtherErrors_KeepsDefaultBackOff() {
        // given
        List<Exception> failures = List.of(
                listenerFailure(new CannotAcquireLockException("Bloqueo no disponible")),
                listenerFailure(new IllegalStateException("Fallo inesperado")),
                listenerFailure(new InvalidHotelSearchMessageException("JSON ilegible", null)),
                listenerFailure(new InvalidHotelSearchException("ages es obligatorio")));

        // when
        List<BackOff> backOffs = failures.stream().map(KafkaConsumerConfiguration::backOffFor).toList();

        // then
        assertAll(Stream.iterate(0, index -> index + 1).limit(failures.size()).map(index -> (Executable) () ->
                assertNull(backOffs.get(index),
                        "No debería haber back-off propio para " + failures.get(index).getCause())));
    }

    /**
     * Verifica que con Oracle caído el registro no va al DLT aunque se
     * superen los cuatro intentos del camino finito.
     *
     * Condiciones:
     * - CannotGetJdbcConnectionException (subtipo de
     *   DataAccessResourceFailureException) en seis intentos seguidos
     * - Container detenido para que el back-off no duerma
     *
     * Se espera:
     * - Ningún intento recupera el registro
     * - El recoverer nunca se invoca
     */
    @Test
    void hotelSearchErrorHandler_InfrastructureUnavailable_NeverRecovers() {
        // given
        List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
        DefaultErrorHandler errorHandler = configuration.hotelSearchErrorHandler(
                (record, exception) -> recovered.add(record));
        ConsumerRecord<String, byte[]> record = record(6);
        Exception failure = listenerFailure(new CannotGetJdbcConnectionException("Oracle no disponible"));

        // when
        List<Boolean> attempts = handleRepeatedly(errorHandler, failure, record, INFRASTRUCTURE_ATTEMPTS);

        // then
        assertAll(
                () -> assertTrue(attempts.stream().noneMatch(Boolean::booleanValue),
                        "Con Oracle caído el registro no debería darse por recuperado: " + attempts),
                () -> assertTrue(recovered.isEmpty(), "Con Oracle caído nada debería ir al DLT"));
    }

    /**
     * Verifica que cuando Oracle vuelve y el mismo registro falla por otro
     * motivo, la cuenta finita empieza de cero en lugar de heredar los
     * intentos de la caída.
     *
     * Condiciones:
     * - Seis fallos por Oracle caído
     * - Después, bloqueo no disponible en cada intento
     *
     * Se espera:
     * - Ninguno de los fallos por caída recupera el registro
     * - El error de bloqueo va al DLT recién en su cuarto intento
     */
    @Test
    void hotelSearchErrorHandler_OutageThenTransientError_RestartsFiniteRetries() {
        // given
        List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
        DefaultErrorHandler errorHandler = configuration.hotelSearchErrorHandler(
                (record, exception) -> recovered.add(record));
        ConsumerRecord<String, byte[]> record = record(7);
        Exception outage = listenerFailure(new CannotCreateTransactionException("Oracle no disponible"));
        Exception lock = listenerFailure(new CannotAcquireLockException("Bloqueo no disponible"));

        // when
        List<Boolean> outageAttempts = handleRepeatedly(errorHandler, outage, record, INFRASTRUCTURE_ATTEMPTS);
        List<Boolean> lockAttempts = handleRepeatedly(errorHandler, lock, record, 4);

        // then
        assertAll(
                () -> assertTrue(outageAttempts.stream().noneMatch(Boolean::booleanValue),
                        "La caída no debería recuperar el registro: " + outageAttempts),
                () -> assertEquals(List.of(false, false, false, true), lockAttempts,
                        "El error de bloqueo debería tener sus tres reintentos completos"),
                () -> assertEquals(List.of(record), recovered, "El registro debería ir una vez al DLT"));
    }

    /**
     * Verifica que el recoverer publica en hotel_search_dlt los bytes
     * originales con la misma key, sin fijar la partición.
     *
     * Condiciones:
     * - Registro fallido del topic de búsquedas
     *
     * Se espera:
     * - Un registro en hotel_search_dlt con la key y el payload originales
     * - Sin partición fija
     */
    @Test
    void deadLetterRecoverer_FailedRecord_PublishesOriginalBytesToDlt() {
        // given
        MockProducer<String, byte[]> producer = mockProducer();
        DeadLetterPublishingRecoverer recoverer = KafkaConsumerConfiguration.deadLetterRecoverer(
                new KafkaTemplate<>(new MockProducerFactory<>(() -> producer)), DLT);
        ConsumerRecord<String, byte[]> record = record(4);

        // when
        recoverer.accept(record, listenerFailure(new InvalidHotelSearchException("ages es obligatorio")));

        // then
        List<ProducerRecord<String, byte[]>> published = producer.history();
        assertEquals(1, published.size(), "Debería publicarse un único dead letter");
        ProducerRecord<String, byte[]> deadLetter = published.getFirst();
        assertAll(
                () -> assertEquals(DLT, deadLetter.topic(), "El dead letter debería ir a hotel_search_dlt"),
                () -> assertNull(deadLetter.partition(), "La partición la debería elegir el producer"),
                () -> assertEquals(record.key(), deadLetter.key(), "Debería conservarse la key original"),
                () -> assertArrayEquals(PAYLOAD, deadLetter.value(), "Deberían conservarse los bytes originales"));
    }

    /**
     * Verifica que si el DLT no confirma la publicación el registro no se da
     * por recuperado, de modo que el offset original no se confirma.
     *
     * Condiciones:
     * - Error permanente
     * - El producer del DLT falla al enviar
     *
     * Se espera:
     * - handleOne informa que el registro no se recuperó
     */
    @Test
    void hotelSearchErrorHandler_DeadLetterPublishFails_DoesNotRecover() {
        // given
        MockProducer<String, byte[]> producer = mockProducer();
        producer.sendException = new KafkaException("DLT no disponible");
        ConsumerRecordRecoverer recoverer = KafkaConsumerConfiguration.deadLetterRecoverer(
                new KafkaTemplate<>(new MockProducerFactory<>(() -> producer)), DLT);
        DefaultErrorHandler errorHandler = configuration.hotelSearchErrorHandler(recoverer);

        // when
        boolean recovered = errorHandler.handleOne(
                listenerFailure(new InvalidHotelSearchException("ages es obligatorio")),
                record(5), consumer, container);

        // then
        assertFalse(recovered, "Sin confirmación del DLT el registro no debería darse por recuperado");
    }

    // <editor-fold desc="Helpers">
    private ConsumerRecord<String, byte[]> record(long offset) {
        return new ConsumerRecord<>(SEARCHES_TOPIC, 0, offset, "3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70", PAYLOAD);
    }

    private Exception listenerFailure(Exception cause) {
        return new ListenerExecutionFailedException("Listener falló", cause);
    }

    private List<Boolean> handleRepeatedly(DefaultErrorHandler errorHandler, Exception failure,
            ConsumerRecord<String, byte[]> record, int times) {
        List<Boolean> attempts = new ArrayList<>();
        for (int attempt = 0; attempt < times; attempt++) {
            attempts.add(errorHandler.handleOne(failure, record, consumer, container));
        }
        return attempts;
    }

    private static List<Long> intervals(BackOff backOff) {
        BackOffExecution execution = backOff.start();
        return Stream.generate(execution::nextBackOff).limit(BACK_OFF_SAMPLES).toList();
    }

    private MockProducer<String, byte[]> mockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    }
    // </editor-fold>
}
