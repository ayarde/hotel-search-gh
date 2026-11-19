package com.hotelsearch;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

class HotelSearchApplicationTest {

    /**
     * Verifica que la aplicación arranca con su configuración por defecto.
     *
     * Condiciones:
     * - Sin dependencias externas configuradas
     * - Puerto HTTP aleatorio para no chocar con otros procesos
     *
     * Se espera:
     * - Contexto de Spring activo y en ejecución
     * - Bean principal de la aplicación registrado
     */
    @Test
    void run_DefaultConfiguration_StartsContext() {
        // given
        String[] args = {"--server.port=0"};

        // when
        try (ConfigurableApplicationContext context =
                SpringApplication.run(HotelSearchApplication.class, args)) {

            // then
            assertAll(
                    () -> assertTrue(context.isActive(), "El contexto debería estar activo"),
                    () -> assertTrue(context.isRunning(), "El contexto debería estar en ejecución"),
                    () -> assertTrue(context.containsBean("hotelSearchApplication"),
                            "Debería existir el bean principal de la aplicación"));
        }
    }
}
