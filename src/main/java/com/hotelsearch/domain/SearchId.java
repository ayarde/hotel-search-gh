package com.hotelsearch.domain;

import java.util.UUID;

/**
 * Identificador de una búsqueda. Lo genera el servidor al aceptarla y se usa
 * como key del mensaje Kafka y como clave primaria en Oracle.
 */
public record SearchId(UUID value) {

    public SearchId {
        Require.nonNull(value, "searchId");
    }

    /**
     * Genera un identificador nuevo (UUIDv4). Dos búsquedas idénticas reciben
     * identificadores distintos.
     */
    public static SearchId generate() {
        return new SearchId(UUID.randomUUID());
    }

    public static SearchId of(String text) {
        return new SearchId(CanonicalUuid.parse(text, "searchId"));
    }
}
