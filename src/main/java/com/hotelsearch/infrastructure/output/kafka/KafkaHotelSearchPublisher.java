package com.hotelsearch.infrastructure.output.kafka;

import com.hotelsearch.application.port.out.HotelSearchPublicationException;
import com.hotelsearch.application.port.out.HotelSearchPublisher;
import com.hotelsearch.domain.HotelSearchEvent;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publica cada búsqueda en Kafka con el {@code searchId} como key y bloquea
 * hasta que el broker la confirma.
 *
 * <p>Los reintentos y los tiempos máximos los resuelve el propio producer
 * ({@code acks=all}, idempotencia, {@code delivery.timeout.ms},
 * {@code max.block.ms}); acá solo se espera el resultado y se traduce
 * cualquier falta de confirmación a {@link HotelSearchPublicationException}.
 * No participa de transacciones Oracle: la persistencia ocurre después, en
 * el consumer.
 */
public class KafkaHotelSearchPublisher implements HotelSearchPublisher {

    private final KafkaTemplate<String, HotelSearchMessage> kafkaTemplate;
    private final String topic;

    public KafkaHotelSearchPublisher(KafkaTemplate<String, HotelSearchMessage> kafkaTemplate, String topic) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
        this.topic = Objects.requireNonNull(topic, "topic");
    }

    @Override
    public void publish(HotelSearchEvent event) {
        String searchId = event.searchId().value().toString();
        try {
            kafkaTemplate.send(topic, searchId, HotelSearchMessage.from(event)).get();
        } catch (ExecutionException e) {
            throw notConfirmed(searchId, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw notConfirmed(searchId, e);
        } catch (KafkaException | org.springframework.kafka.KafkaException e) {
            // send() falla sin devolver future: el cliente lanza su propia
            // excepción al serializar y KafkaTemplate envuelve los fallos
            // inmediatos, como no obtener metadata antes de max.block.ms.
            throw notConfirmed(searchId, e);
        } catch (IllegalStateException e) {
            // Producer ya cerrado (shutdown del contexto) o future cancelado:
            // CancellationException extiende IllegalStateException.
            throw notConfirmed(searchId, e);
        }
    }

    private static HotelSearchPublicationException notConfirmed(String searchId, Throwable cause) {
        return new HotelSearchPublicationException(
                "Kafka no confirmó la publicación de la búsqueda " + searchId, cause);
    }
}
