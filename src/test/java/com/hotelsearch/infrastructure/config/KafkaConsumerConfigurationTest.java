package com.hotelsearch.infrastructure.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hotelsearch.domain.InvalidHotelSearchException;
import com.hotelsearch.infrastructure.input.kafka.InvalidHotelSearchMessageException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.mock.MockProducerFactory;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigurationTest {

    private static final String SEARCHES_TOPIC = "hotel_availability_searches";
    private static final String DLT = "hotel_search_dlt";
    private static final byte[] PAYLOAD = "{\"searchId\":\"x\"}".getBytes(UTF_8);

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
     * Verifica que un error transitorio se reintenta tres veces más antes de
     * ir al DLT.
     *
     * Condiciones:
     * - Oracle no disponible en todos los intentos
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
        Exception failure = listenerFailure(new DataAccessResourceFailureException("Oracle no disponible"));
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

    private MockProducer<String, byte[]> mockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    }
    // </editor-fold>
}
