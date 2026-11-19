package com.hotelsearch.infrastructure.output.persistence;

import com.hotelsearch.domain.Ages;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Traduce las edades al JSON compacto de la columna {@code ages} y de vuelta.
 *
 * <p>El formato es fijo, por ejemplo {@code [29,28,4,3]}: sin espacios y en el
 * orden de la búsqueda. Así la igualdad de texto en SQL equivale a la igualdad
 * de las listas, que es lo que necesita el conteo de búsquedas equivalentes.
 */
final class AgesColumn {

    private AgesColumn() {
    }

    static String toJson(Ages ages) {
        return ages.values().stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * Lee el valor tal como lo escribe {@link #toJson(Ages)}. Cualquier otro
     * formato indica datos que no salieron de esta aplicación y se rechaza.
     */
    static List<Integer> fromJson(String json) {
        if (json == null || json.length() < 3 || json.charAt(0) != '[' || json.charAt(json.length() - 1) != ']') {
            throw invalid(json, null);
        }
        try {
            return Arrays.stream(json.substring(1, json.length() - 1).split(",", -1))
                    .map(Integer::valueOf)
                    .toList();
        } catch (NumberFormatException e) {
            throw invalid(json, e);
        }
    }

    private static IllegalStateException invalid(String json, Throwable cause) {
        return new IllegalStateException("La columna ages tiene un valor inesperado: " + json, cause);
    }
}
