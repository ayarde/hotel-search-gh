package com.hotelsearch;

import java.time.Duration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Oracle AI Database Free para los integration tests.
 *
 * <p>Se usa la imagen oficial de Oracle, la misma de Docker Compose. El módulo
 * de Testcontainers para Oracle solo acepta la imagen comunitaria, así que el
 * contenedor es un {@link GenericContainer} y la conexión se registra con
 * {@code @DynamicPropertySource}. Cada clase de test declara su propio
 * contenedor con {@link #create()}.
 */
public final class OracleTestContainer {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("container-registry.oracle.com/database/free:23.26.3.0-lite");
    private static final int ORACLE_PORT = 1521;
    private static final String APP_USER = "hotel_search";
    private static final String APP_PASSWORD = "hotel_search_it";
    private static final String APP_USER_SCRIPT = "/opt/oracle/scripts/startup/01-create-app-user.sql";

    // La imagen ejecuta los scripts de /opt/oracle/scripts/startup después de
    // anunciar que la base está lista; este mensaje sale cuando ya terminaron.
    private static final String SCRIPTS_FINISHED_LOG = ".*The following output is now a tail of the alert\\.log:.*\\n";

    private OracleTestContainer() {
    }

    public static GenericContainer<?> create() {
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(ORACLE_PORT)
                .withCopyToContainer(MountableFile.forClasspathResource("oracle/startup/01-create-app-user.sql"),
                        APP_USER_SCRIPT)
                .waitingFor(Wait.forLogMessage(SCRIPTS_FINISHED_LOG, 1))
                .withStartupTimeout(Duration.ofMinutes(5));
    }

    public static void registerDataSource(DynamicPropertyRegistry registry, GenericContainer<?> oracle) {
        registry.add("spring.datasource.url", () -> "jdbc:oracle:thin:@//%s:%d/FREEPDB1"
                .formatted(oracle.getHost(), oracle.getMappedPort(ORACLE_PORT)));
        registry.add("spring.datasource.username", () -> APP_USER);
        registry.add("spring.datasource.password", () -> APP_PASSWORD);
    }
}
