# TESTING_RULES.md — Reglas de testing para Hotel Search

Este documento define cómo deben escribirse y organizarse los tests del
proyecto. El agente de coding (Pi) **debe aplicar estas reglas de forma
estricta** en todas las fases que generen o modifiquen tests.

Complementa `AGENTS.md`, `TECHNICAL-DECISIONS.md` e
`IMPLEMENTATION-PLAN.md`.

---

## 1. Estructura de Tests (Given / When / Then)

Todo test debe seguir la estructura **given / when / then**:

```java
@Test
void methodUnderTesting_StateUnderTest() {
    // given
    // preparación de datos, mocks y contexto

    // when
    // invocación del método bajo prueba (nunca un mock)

    // then
    // asserts y verifies
}
```

No mezclar preparación, ejecución y verificación en el mismo bloque.

---

## 2. Naming Convention

Formato obligatorio:

```text
methodUnderTesting_StateUnderTest
```

- `methodUnderTesting`: nombre exacto del método que se prueba.
- `StateUnderTest`: escenario concreto (Success, NotFound, InvalidAges,
  KafkaUnavailable, etc.).

Ejemplos válidos:

```java
void submit_ValidRequest_ReturnsSearchId()
void count_SearchNotFound_ThrowsNotFound()
void persist_DuplicateSearchId_IsIgnored()
void publish_BrokerDown_ThrowsServiceUnavailable()
```

Los nombres de los métodos de test van en **inglés**.

---

## 3. Idioma

| Elemento                   | Idioma  |
|----------------------------|---------|
| Nombres de métodos de test | Inglés  |
| Javadoc y comentarios      | Español |
| Mensajes de assert         | Español |
| Variables de test          | Inglés o español, según claridad |

No usar nombres de test en español.

---

## 4. Comentarios (Javadoc)

Todo test público debe llevar Javadoc en español que explique:

- Qué escenario se prueba.
- Condiciones relevantes.
- Comportamiento esperado.

Ejemplo:

```java
/**
 * Verifica que una búsqueda válida se publica en Kafka y se responde
 * con 202 Accepted y el searchId generado.
 *
 * Condiciones:
 * - Payload correcto
 * - Broker disponible
 *
 * Se espera:
 * - Status 202
 * - Body con searchId no nulo
 * - Evento publicado con key = searchId
 */
@Test
void submit_ValidRequest_ReturnsAccepted() {
    // ...
}
```

El tono debe ser el de un Senior Software Engineer: directo, preciso y
sin formulaciones genéricas o típicas de IA.

---

## 5. Organización de Métodos

1. Primero todos los métodos de test (públicos).
2. Al final, métodos auxiliares privados agrupados:

```java
// <editor-fold desc="Helpers">
private HotelSearchRequest validRequest() { ... }
private void assertSearchPersisted(String searchId) { ... }
// </editor-fold>
```

No intercalar helpers entre tests.

---

## 6. Manejo de Excepciones

- Cuando el test espera una excepción concreta, preferir
  `assertThrows`:

```java
assertThrows(SearchNotFoundException.class,
        () -> countHotelSearch.count(unknownId));
```

- Si además hay que validar mensaje o estado, usar el valor devuelto
  por `assertThrows`.
- No usar `@Test(expected = ...)` (estilo JUnit 4).

---

## 7. Tipos de Tests y ubicación

| Tipo              | Ubicación          | Dependencias          |
|-------------------|--------------------|-----------------------|
| Unit tests        | `src/test/java`    | Mocks (Mockito)       |
| Integration tests | `src/intTest/java` | Kafka + Oracle reales |

- Los unit tests no deben levantar contenedores ni usar Testcontainers.
- Los integration tests usan Testcontainers con Kafka y Oracle.
- No usar H2 como sustituto de Oracle.

---

## 8. Testcontainers (reglas obligatorias)

1. **Versión de imagen pinneada**. Nunca `:latest`.
2. **No hardcodear host ni puertos**. Usar `getHost()`,
   `getMappedPort()`, `getJdbcUrl()`, etc.
3. **Wait strategy explícita** en todos los containers.
4. **Startup timeout** razonable (especialmente en CI).
5. Preferir módulos oficiales antes que `GenericContainer`.
6. Un container por clase de test (o Singleton bien implementado). No
   mezclar `@Testcontainers`/`@Container` con Singleton.
7. Limpiar estado entre tests cuando se comparte el container.
8. **Reuse solo en local**. Prohibido en CI.
9. Resource Reaper (Ryuk) siempre activo.
10. Integration tests **solo** en `src/intTest/java`.

Patrón preferido con Spring Boot:

```java
@Testcontainers
@SpringBootTest
class HotelSearchRepositoryIT {

    @Container
    @ServiceConnection
    static /* container con versión pinneada + wait */ oracle = ...;

    @Container
    @ServiceConnection
    static /* KafkaContainer pinneado */ kafka = ...;
}
```

Si `@ServiceConnection` no aplica, usar `@DynamicPropertySource`.

---

## 9. Cobertura y calidad

- JaCoCo mínimo 80% en branches y líneas sobre código de producción.
- No inflar cobertura con tests triviales.
- Usar `assertAll` cuando convenga agrupar asserts independientes.
- Cubrir casos positivos, negativos, límites y, cuando aplique,
  concurrencia / redelivery.

---

## 10. Comandos de ejecución

```bash
# Habitual: compila + unit tests
./gradlew build

# Completo: unit + integration tests (Testcontainers)
./gradlew build -PintegrationTest=true
```

- Los integration tests **no** se ejecutan por defecto.
- El flag de activación es `-PintegrationTest=true`.

---

## 11. Checklist rápido

- [ ] Given / When / Then respetado
- [ ] Naming `method_State` en inglés
- [ ] Javadoc en español, tono natural de Senior
- [ ] Helpers al final con editor-fold
- [ ] Unit tests en `src/test`, integration en `src/intTest`
- [ ] Testcontainers: imagen pinneada, wait strategy, sin puertos
      hardcodeados, limpieza de estado
- [ ] Comandos de build ejecutados y resultados reales reportados
- [ ] Cobertura no por debajo del umbral cuando corresponda a la fase

---

## 12. Fuera de alcance de estas reglas

- No sustituyen los criterios de aceptación de cada fase del
  `IMPLEMENTATION-PLAN.md`.
- No se permite usar H2 ni mocks de broker/base de datos en los
  integration tests.
