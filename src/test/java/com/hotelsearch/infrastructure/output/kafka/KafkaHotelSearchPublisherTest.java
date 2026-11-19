package com.hotelsearch.infrastructure.output.kafka;

import static com.hotelsearch.HotelSearchFixtures.SEARCH_ID;
import static com.hotelsearch.HotelSearchFixtures.event;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hotelsearch.application.port.out.HotelSearchPublicationException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Cubre la espera del ACK y la traducción de fallos del producer a
 * {@link HotelSearchPublicationException}. La publicación contra un broker
 * real se prueba en {@code KafkaHotelSearchPublisherIT}.
 */
@ExtendWith(MockitoExtension.class)
class KafkaHotelSearchPublisherTest {

    private static final String TOPIC = "hotel_availability_searches";
    private static final String KEY = SEARCH_ID.value().toString();

    @Mock
    private KafkaTemplate<String, HotelSearchMessage> kafkaTemplate;

    private KafkaHotelSearchPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new KafkaHotelSearchPublisher(kafkaTemplate, TOPIC);
    }

    /**
     * Verifica que el evento se envía al topic configurado, con el searchId
     * como key y el payload armado desde el evento.
     *
     * Condiciones:
     * - El broker confirma el envío
     *
     * Se espera:
     * - Un único send(topic, searchId, mensaje)
     * - publish retorna sin error
     */
    @Test
    void publish_BrokerConfirms_SendsMessageKeyedBySearchId() {
        // given
        when(kafkaTemplate.send(anyString(), anyString(), any(HotelSearchMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // when
        publisher.publish(event());

        // then
        verify(kafkaTemplate).send(TOPIC, KEY, HotelSearchMessage.from(event()));
    }

    /**
     * Verifica que publish no retorna mientras el broker no confirmó el
     * envío; es lo que impide que el POST informe éxito antes del ACK.
     *
     * Condiciones:
     * - El future del envío sigue pendiente
     *
     * Se espera:
     * - publish sigue bloqueado mientras el future no se completa
     * - publish termina sin error una vez que llega el ACK
     */
    @Test
    void publish_AckPending_BlocksUntilBrokerConfirms() throws InterruptedException {
        // given
        CompletableFuture<SendResult<String, HotelSearchMessage>> ack = new CompletableFuture<>();
        when(kafkaTemplate.send(TOPIC, KEY, HotelSearchMessage.from(event()))).thenReturn(ack);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        // when
        Thread publishing = Thread.ofVirtual().start(() -> {
            try {
                publisher.publish(event());
            } catch (RuntimeException e) {
                failure.set(e);
            }
        });
        boolean returnedBeforeAck = publishing.join(Duration.ofMillis(200));
        ack.complete(null);
        boolean returnedAfterAck = publishing.join(Duration.ofSeconds(5));

        // then
        assertAll(
                () -> assertFalse(returnedBeforeAck, "publish no debería retornar antes del ACK del broker"),
                () -> assertTrue(returnedAfterAck, "publish debería retornar al recibir el ACK"),
                () -> assertNull(failure.get(), "Un envío confirmado no debería fallar"));
    }

    /**
     * Verifica que un envío que el broker no confirma (por ejemplo, porque
     * venció delivery.timeout.ms) se traduce al error del puerto.
     *
     * Condiciones:
     * - El future del envío termina con TimeoutException de Kafka
     *
     * Se espera:
     * - HotelSearchPublicationException
     * - La causa es la excepción original del producer
     * - El mensaje identifica la búsqueda
     */
    @Test
    void publish_DeliveryTimesOut_ThrowsPublicationException() {
        // given
        TimeoutException timeout = new TimeoutException("Expiring 1 record(s)");
        when(kafkaTemplate.send(TOPIC, KEY, HotelSearchMessage.from(event())))
                .thenReturn(CompletableFuture.failedFuture(timeout));

        // when
        Executable action = () -> publisher.publish(event());

        // then
        HotelSearchPublicationException thrown = assertThrows(HotelSearchPublicationException.class, action,
                "Un envío sin confirmar debería informarse como fallo de publicación");
        assertAll(
                () -> assertSame(timeout, thrown.getCause(), "La causa debería ser el error del producer"),
                () -> assertTrue(thrown.getMessage().contains(KEY), "El mensaje debería incluir el searchId"));
    }

    /**
     * Verifica que un fallo de send() previo al envío, sin future, también se
     * traduce al error del puerto.
     *
     * Condiciones:
     * - send() lanza SerializationException
     *
     * Se espera:
     * - HotelSearchPublicationException con la excepción original como causa
     */
    @Test
    void publish_SendFailsImmediately_ThrowsPublicationException() {
        // given
        SerializationException serializationError = new SerializationException("No se pudo serializar");
        when(kafkaTemplate.send(eq(TOPIC), eq(KEY), any(HotelSearchMessage.class))).thenThrow(serializationError);

        // when
        Executable action = () -> publisher.publish(event());

        // then
        HotelSearchPublicationException thrown = assertThrows(HotelSearchPublicationException.class, action,
                "Un error de send() debería informarse como fallo de publicación");
        assertSame(serializationError, thrown.getCause(), "La causa debería ser el error de send()");
    }

    /**
     * Verifica que el fallo inmediato que KafkaTemplate envuelve en su propia
     * excepción (por ejemplo, sin metadata del topic antes de max.block.ms)
     * también se traduce al error del puerto.
     *
     * Condiciones:
     * - send() lanza org.springframework.kafka.KafkaException
     *
     * Se espera:
     * - HotelSearchPublicationException con la excepción original como causa
     */
    @Test
    void publish_TemplateFailsImmediately_ThrowsPublicationException() {
        // given
        org.springframework.kafka.KafkaException sendFailed = new org.springframework.kafka.KafkaException(
                "Send failed", new TimeoutException("Topic not present in metadata after 5000 ms."));
        when(kafkaTemplate.send(eq(TOPIC), eq(KEY), any(HotelSearchMessage.class))).thenThrow(sendFailed);

        // when
        Executable action = () -> publisher.publish(event());

        // then
        HotelSearchPublicationException thrown = assertThrows(HotelSearchPublicationException.class, action,
                "Un fallo inmediato de KafkaTemplate debería informarse como fallo de publicación");
        assertSame(sendFailed, thrown.getCause(), "La causa debería ser el error de KafkaTemplate");
    }

    /**
     * Verifica que una interrupción mientras se espera el ACK no se pierde:
     * se informa como publicación no confirmada y el thread queda marcado
     * como interrumpido.
     *
     * Condiciones:
     * - Future pendiente
     * - Thread interrumpido antes de esperar
     *
     * Se espera:
     * - HotelSearchPublicationException con InterruptedException como causa
     * - El flag de interrupción sigue activo
     */
    @Test
    void publish_InterruptedWhileWaiting_ThrowsAndKeepsInterruptFlag() {
        // given
        when(kafkaTemplate.send(TOPIC, KEY, HotelSearchMessage.from(event()))).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();

        // when
        Executable action = () -> publisher.publish(event());

        // then
        HotelSearchPublicationException thrown = assertThrows(HotelSearchPublicationException.class, action,
                "Una espera interrumpida debería informarse como fallo de publicación");
        boolean stillInterrupted = Thread.interrupted();
        assertAll(
                () -> assertInstanceOf(InterruptedException.class, thrown.getCause(),
                        "La causa debería ser la interrupción"),
                () -> assertTrue(stillInterrupted, "El flag de interrupción debería restaurarse"));
    }
}
