# AGENTS.md — Guía de trabajo para agentes de coding

## 1. Propósito

Implementar **Hotel Search**, una aplicación Java para recibir búsquedas
hoteleras, publicarlas en Kafka, persistirlas asincrónicamente en Oracle
y consultar la cantidad de búsquedas equivalentes.

Este archivo establece cómo debe trabajar el agente (Pi). Los requisitos
funcionales están en `REQUIREMENTS.md`, las decisiones cerradas en
`TECHNICAL-DECISIONS.md`, el orden de trabajo en
`IMPLEMENTATION-PLAN.md` y las reglas de testing en `TESTING_RULES.md`.

Antes de proponer o implementar cualquier cambio de diseño, el agente debe
contrastarlo con `REQUIREMENTS.md` y `TECHNICAL-DECISIONS.md`.

- Si el cambio contradice un requisito o una decisión cerrada, **no
  implementarlo**.
- Debe detenerse, explicar el conflicto con evidencia y proponer
  alternativas.
- Solo puede proceder si el usuario autoriza explícitamente el cambio.
- No “mejorar” el diseño en silencio ni reinterpretar decisiones para
  facilitar la implementación.

Antes de modificar código: 1. Leer los cinco documentos. 2.
Inspeccionar el estado real del repositorio. 3. Identificar la fase
activa del plan. 4. No asumir que una tarea está terminada solo porque
existe una clase o archivo.

**Testing:** Toda prueba unitaria o de integración debe cumplir
`TESTING_RULES.md`. El agente debe aplicar esas reglas de forma
estricta (estructura given/when/then, naming, idioma, Javadoc,
organización de métodos y reglas de Testcontainers).

## 2. Principios obligatorios

- Group/artifact: `com.hotelsearch` / `hotel-search`. Paquete base:
  `com.hotelsearch`.
- Java 25 LTS, Spring Boot 4.1.x estable compatible, Gradle Wrapper y
  Kotlin DSL.
- No usar Maven ni generar `pom.xml`.
- Arquitectura hexagonal en un único proyecto Gradle; no crear
  multi-project builds ni módulos Gradle.
- Mantener las dependencias apuntando hacia el dominio.
- Mantener `domain` libre de dependencias de Spring, Kafka, Oracle,
  JDBC y Jackson.
- Usar Spring JDBC para Oracle.
- Usar Spring Kafka para producer y consumer.
- Habilitar Virtual Threads.
- Usar Testcontainers para pruebas de integración con Kafka y Oracle
  reales. Los integration tests viven en `src/intTest/java`.
- Usar Flyway para migraciones de esquema.
- Mantener identificadores SQL (tablas, columnas, índices y
  constraints) en minúsculas.
- Mantener inmutables los modelos de dominio y eventos; usar records
  cuando sea apropiado y copias defensivas para colecciones.
- No internacionalizar mensajes. Usar textos claros en un solo idioma.
- No añadir complejidad que no esté justificada por los requisitos.
- Aplicar SOLID, KISS, YAGNI y DRY con criterio práctico:
  - SOLID: responsabilidades claras, dependencias hacia abstracciones
    y contratos bien definidos.
  - KISS: preferir la solución más simple que satisfaga los requisitos.
  - YAGNI: no implementar funcionalidades, abstracciones ni extensibilidad
    especulativas.
  - DRY: evitar duplicar reglas de negocio, validaciones y lógica;
    no crear abstracciones artificiales para eliminar duplicaciones
    superficiales.
- Aplicar ACID a las transacciones Oracle. No asumir atomicidad
  distribuida entre Kafka y Oracle; respetar el modelo de consistencia
  eventual y entrega at-least-once definido para el proyecto.
- Comentarios, Javadocs y textos de documentación deben sonar naturales,
  como los escribiría un Senior Software Engineer. Evitar tono genérico
  o típico de IA.

## 3. Decisiones que NO deben reintroducirse

No implementar, salvo que el usuario cambie explícitamente el diseño:

- Idempotencia HTTP ni cabecera `Idempotency-Key`.
- Tabla de idempotencia HTTP.
- Outbox pattern.
- Leases, locks de publicación, tokens o publisher recovery.
- Scheduler de recuperación o cleanup.
- Exactly-once entre Kafka y Oracle.
- XA o transacciones distribuidas.
- Tabla de contadores.
- Reprocesamiento automático del DLT.
- Failover automático entre proveedores.
- Módulos Gradle separados.
- Publicación del artefacto en Maven Central o en otro registry.
- Internacionalización de mensajes.
- Backend de tracing (Jaeger, Zipkin, OTLP exporter) en el challenge.

El objetivo es una solución clara y suficiente para el challenge, no
construir una plataforma genérica.

## 4. Reglas de arquitectura

Paquetes conceptuales:

- `com.hotelsearch.domain`: entidades, value objects, reglas e invariantes.
- `com.hotelsearch.application`: casos de uso y puertos.
- `com.hotelsearch.infrastructure.input.rest`: controllers, DTOs,
  validación HTTP y mapeos.
- `com.hotelsearch.infrastructure.input.kafka`: listener, configuración
  del consumer y DLT.
- `com.hotelsearch.infrastructure.output.kafka`: implementación del
  publisher.
- `com.hotelsearch.infrastructure.output.persistence`: implementación
  JDBC del repositorio.
- `com.hotelsearch.infrastructure.config`: configuración Spring, Kafka,
  JDBC, Flyway y Virtual Threads.

Casos de uso:
- `SubmitHotelSearch`: validar reglas de negocio aplicables, crear
  `searchId` y publicar el evento.
- `CountHotelSearch`: recuperar la búsqueda y contar coincidencias
  excluyendo la propia búsqueda.
- `PersistHotelSearch`: persistir el evento recibido por Kafka.

Puertos mínimos:
- `HotelSearchPublisher`.
- `HotelSearchRepository`.

Crear puertos en las fronteras
donde la aplicación necesita tecnología externa.

## 5. Reglas de implementación

- Preferir soluciones simples, explícitas y testeables.
- No concatenar Strings con `+` dentro de loops.
- No interpolar valores del usuario en SQL; usar parámetros bind de
  JDBC.
- No usar `java.util.Date`; usar `LocalDate` para fechas y tipos
  `java.time` para timestamps.
- No hacer conversiones costosas o inesperadas dentro de getters.
- No mutar listas recibidas o expuestas.
- No capturar excepciones genéricas para ocultar fallos.
- No descartar errores de persistencia.
- Distinguir duplicado por PK (`search_id`) de cualquier otro error
  SQL.
- No hacer un SELECT previo al INSERT para prevenir duplicados:
  insertar directamente y tratar la PK como defensa de redelivery.
- No confirmar el offset Kafka antes del commit Oracle.
- No mantener una transacción Oracle abierta mientras se espera una
  publicación Kafka.
- No responder éxito al POST antes del ACK de Kafka.
- No afirmar que el POST garantiza persistencia inmediata en Oracle:
  el consumer trabaja asincrónicamente.
- El POST responde **202 Accepted**.

## 6. Flujo de trabajo por implementación

Trabajar en incrementos pequeños y verificables. Antes de implementar
cada fase, presentar o registrar:

1. **Alcance:** qué se va a implementar y qué queda explícitamente
   fuera.
2. **Criterios de aceptación:** resultados observables que deben
   cumplirse.
3. **Plan de cambios:** paquetes, clases, configuración y archivos que
   se modificarán.
4. **Validación:** comandos y pruebas que demostrarán que está
   correcto.

Después de implementar:
- Ejecutar las validaciones definidas.
- Revisar el diff completo.
- Informar archivos modificados, decisiones tomadas, resultados reales
  de pruebas y cualquier limitación. Solo utilizá títulos, subtítulos,
  párrafos y listas. No uses tablas.
- No marcar una fase como terminada si falló una validación obligatoria.
- No inventar resultados de tests ni cobertura.
- Si una decisión documentada parece técnicamente inviable, detenerse,
  explicar evidencia y proponer alternativas; no cambiarla
  silenciosamente.

## 7. Calidad y testing

- JUnit 5 y Mockito para pruebas unitarias.
- Testcontainers para Kafka y Oracle en integración.
- No reemplazar la integración real con mocks ni con H2.
- Tests unitarios en `src/test/java`; integration tests en
  `src/intTest/java`.
- JaCoCo: mínimo 80% en branches y líneas,
  medido sobre el código de producción.
- Aplicar estrictamente `TESTING_RULES.md`.
- Usar `assertAll` cuando permita agrupar verificaciones
  independientes.
- Verificar casos positivos, negativos, límites y concurrencia donde
  corresponda.

Comandos de uso habitual:

```bash
# Build habitual: compila y ejecuta unit tests
./gradlew build

# Build completo: además ejecuta integration tests con Testcontainers
./gradlew build -PintegrationTest=true
```

## 8. Docker y ejecución local

El entorno local se levanta con:

```bash
docker compose up --build
```

Servicios:
- aplicación Spring Boot;
- Apache Kafka en KRaft, un broker;
- Oracle AI Database Free.

La aplicación debe esperar a que Kafka y Oracle estén saludables. No
usar `depends_on` sin health checks como sustituto de readiness.

El Dockerfile será multi-stage: build con Gradle Wrapper y JDK 25;
runtime con JRE 25, usuario no root y solo el artefacto ejecutable.

No incluir secretos reales en Git. Mantener `.env` ignorado y documentar
variables requeridas en `.env.example`.

## 9. Dependencias y versiones

Centralizar versiones. Fijar versiones estables compatibles y no usar
`latest` para imágenes de infraestructura. A la fecha de preparación de
estos documentos (10-11-2026), la referencia de trabajo es:
- Java 25 LTS;
- Spring Boot 4.1.2;
- Gradle 9.9.0;
- Apache Kafka 4.4.0;
- Oracle AI Database 26ai Free, imagen
  `container-registry.oracle.com/database/free:23.26.3.0-lite`.

Antes de fijar definitivamente tags o plugins, verificar disponibilidad
real de la etiqueta, compatibilidad de arquitectura y compatibilidad
entre versiones. Si la imagen oficial Oracle indicada no está disponible
para la arquitectura del entorno, detenerse y proponer un tag oficial
multi-arch; no sustituirla silenciosamente por una imagen no oficial.

## 10. Definition of Done por fase

Una fase está terminada solo cuando:
- todos sus criterios de aceptación se cumplen;
- las validaciones declaradas pasan;
- no se rompen fases anteriores;
- no quedan errores conocidos ocultos;
- documentación relevante está actualizada;
- el agente presenta un resumen verificable de los resultados.
- el código respeta SOLID, KISS, YAGNI y DRY, y las operaciones
    transaccionales respetan las garantías ACID aplicables.
