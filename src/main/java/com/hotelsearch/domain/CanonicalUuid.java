package com.hotelsearch.domain;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Interpreta UUIDs escritos en su forma canónica 8-4-4-4-12.
 *
 * <p>{@link UUID#fromString(String)} acepta formas abreviadas como
 * {@code 1-1-1-1-1}, así que el formato se valida antes. Tampoco se recortan
 * espacios: un valor con espacios es un error del cliente.
 */
final class CanonicalUuid {

    private static final Pattern FORMAT = Pattern.compile(
            "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}");

    private CanonicalUuid() {
    }

    static UUID parse(String text, String field) {
        Require.nonNull(text, field);
        if (!FORMAT.matcher(text).matches()) {
            throw new InvalidHotelSearchException(field + " debe ser un UUID canónico de 36 caracteres");
        }
        return UUID.fromString(text);
    }
}
