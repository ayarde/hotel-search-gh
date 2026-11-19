package com.hotelsearch.infrastructure.input.kafka;

import com.hotelsearch.application.port.in.PersistHotelSearchUseCase;
import com.hotelsearch.domain.HotelSearchEvent;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consume {@code hotel_availability_searches} y persiste cada búsqueda en
 * Oracle.
 *
 * <p>La persistencia es síncrona: el método retorna después del commit, y
 * recién ahí el container confirma el offset ({@code AckMode.RECORD}). Las
 * excepciones no se capturan; el error handler decide entre reintentar y
 * enviar al DLT.
 */
public class HotelSearchListener {

    private static final Logger log = LoggerFactory.getLogger(HotelSearchListener.class);

    private final PersistHotelSearchUseCase persistHotelSearch;
    private final JsonMapper jsonMapper;

    public HotelSearchListener(PersistHotelSearchUseCase persistHotelSearch, JsonMapper jsonMapper) {
        this.persistHotelSearch = Objects.requireNonNull(persistHotelSearch, "persistHotelSearch");
        this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
    }

    @KafkaListener(topics = "${hotel-search.kafka.searches-topic}")
    public void onMessage(@Payload(required = false) byte[] payload) {
        HotelSearchEvent event = read(payload).toEvent();
        if (!persistHotelSearch.persist(event)) {
            log.info("Búsqueda {} ya persistida; se descarta la redelivery", event.searchId().value());
        }
    }

    private ConsumedHotelSearchMessage read(byte[] payload) {
        if (payload == null) {
            throw new InvalidHotelSearchMessageException("El mensaje no tiene valor", null);
        }
        ConsumedHotelSearchMessage message;
        try {
            message = jsonMapper.readValue(payload, ConsumedHotelSearchMessage.class);
        } catch (JacksonException e) {
            throw new InvalidHotelSearchMessageException("El mensaje no es una búsqueda legible: "
                    + e.getOriginalMessage(), e);
        }
        if (message == null) {
            throw new InvalidHotelSearchMessageException("El mensaje no tiene valor", null);
        }
        return message;
    }
}
