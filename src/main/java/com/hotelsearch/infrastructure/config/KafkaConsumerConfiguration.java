package com.hotelsearch.infrastructure.config;

import com.hotelsearch.application.port.in.PersistHotelSearchUseCase;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.application.usecase.PersistHotelSearch;
import com.hotelsearch.domain.InvalidHotelSearchException;
import com.hotelsearch.infrastructure.input.kafka.HotelSearchListener;
import com.hotelsearch.infrastructure.input.kafka.InvalidHotelSearchMessageException;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumer de búsquedas: topics, listener, reintentos y DLT.
 *
 * <p>Spring Boot aplica el {@link DefaultErrorHandler} declarado acá al
 * container factory autoconfigurado; {@code AckMode.RECORD} y los
 * deserializers están en {@code spring.kafka.*} de {@code application.yml}.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConsumerConfiguration {

    private static final long RETRY_INTERVAL_MS = 1_000L;
    private static final long ADDITIONAL_ATTEMPTS = 3L;
    private static final double OUTAGE_BACK_OFF_MULTIPLIER = 2.0;
    // El back-off duerme en el hilo del consumer: el tope tiene que quedar
    // muy por debajo de max.poll.interval.ms (300 s por defecto).
    private static final long OUTAGE_MAX_INTERVAL_MS = 30_000L;
    // Con un broker, más particiones o réplicas no aportan nada.
    private static final int PARTITIONS = 1;
    private static final int REPLICAS = 1;
    // Sin partición fija: el producer elige, así el DLT no depende de cuántas
    // particiones tenga el topic de origen.
    private static final int ANY_PARTITION = -1;

    @Bean
    NewTopic hotelSearchesTopic(@Value("${hotel-search.kafka.searches-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic hotelSearchDeadLetterTopic(@Value("${hotel-search.kafka.dead-letter-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    PersistHotelSearchUseCase persistHotelSearch(HotelSearchRepository repository) {
        return new PersistHotelSearch(repository);
    }

    @Bean
    HotelSearchListener hotelSearchListener(PersistHotelSearchUseCase persistHotelSearch) {
        // Mapper propio con la configuración por defecto: el contrato del
        // mensaje no debe cambiar por ajustes de Jackson pensados para HTTP.
        return new HotelSearchListener(persistHotelSearch, JsonMapper.shared());
    }

    /**
     * El DLT recibe los bytes originales, sin volver a serializarlos, sea cual
     * sea el motivo del fallo. Por eso el template usa
     * {@link ByteArraySerializer} sobre el producer factory de Spring Boot.
     */
    @Bean
    DeadLetterPublishingRecoverer deadLetterRecoverer(
            ProducerFactory<String, byte[]> producerFactory,
            @Value("${hotel-search.kafka.dead-letter-topic}") String deadLetterTopic) {
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        return deadLetterRecoverer(template, deadLetterTopic);
    }

    /**
     * Tres reintentos adicionales con 1 segundo de intervalo para errores
     * transitorios; los permanentes van al DLT en el primer intento. Si la
     * publicación en el DLT falla, el registro no se da por recuperado y el
     * offset no se confirma.
     *
     * <p>Oracle no disponible (sin conexión o sin poder iniciar la
     * transacción) es la excepción: una caída de más de unos 3 s mandaría al
     * DLT búsquedas válidas, así que se reintenta sin límite con back-off
     * exponencial de 1 s a 30 s y el offset queda sin confirmar hasta que
     * Oracle vuelve. Como eso frena la única partición, solo se listan tipos
     * de conexión e inicio de transacción; bloqueos, timeouts y errores de
     * datos siguen el camino finito. Si el mismo registro pasa a fallar por
     * otro motivo, el handler reinicia la cuenta con el back-off que
     * corresponda al nuevo error ({@code resetStateOnExceptionChange}, activo
     * por defecto).
     */
    @Bean
    DefaultErrorHandler hotelSearchErrorHandler(ConsumerRecordRecoverer deadLetterRecoverer) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                deadLetterRecoverer, new FixedBackOff(RETRY_INTERVAL_MS, ADDITIONAL_ATTEMPTS));
        errorHandler.addNotRetryableExceptions(
                InvalidHotelSearchMessageException.class, InvalidHotelSearchException.class);
        errorHandler.setBackOffFunction((record, exception) -> backOffFor(exception));
        return errorHandler;
    }

    /**
     * Back-off propio para Oracle no disponible, o {@code null} para que se
     * aplique el del error handler.
     *
     * <p>Se recorre toda la cadena de causas: el container entrega el error
     * envuelto en {@code ListenerExecutionFailedException} y Spring puede
     * anidar la excepción de conexión dentro de otra.
     */
    static BackOff backOffFor(Exception exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (isInfrastructureUnavailable(cause)) {
                ExponentialBackOff backOff = new ExponentialBackOff(RETRY_INTERVAL_MS, OUTAGE_BACK_OFF_MULTIPLIER);
                backOff.setMaxInterval(OUTAGE_MAX_INTERVAL_MS);
                // Sin límite de intentos ni de tiempo: son los valores por defecto.
                return backOff;
            }
        }
        return null;
    }

    private static boolean isInfrastructureUnavailable(Throwable exception) {
        return exception instanceof CannotCreateTransactionException
                || exception instanceof DataAccessResourceFailureException
                || exception instanceof TransientDataAccessResourceException
                || exception instanceof RecoverableDataAccessException;
    }

    static DeadLetterPublishingRecoverer deadLetterRecoverer(
            KafkaOperations<String, byte[]> template, String deadLetterTopic) {
        return new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(deadLetterTopic, ANY_PARTITION));
    }
}
