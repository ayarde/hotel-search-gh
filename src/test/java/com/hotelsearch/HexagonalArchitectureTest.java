package com.hotelsearch;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.INTERFACES;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.EvaluationResult;
import org.junit.jupiter.api.Test;

/**
 * Reglas de dependencia de la arquitectura hexagonal, evaluadas sobre las
 * clases de producción. Un fallo indica que un cambio cruzó una frontera que
 * el diseño no permite; el reporte detalla la clase y la dependencia.
 */
class HexagonalArchitectureTest {

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.hotelsearch");

    /**
     * Verifica que el dominio no conoce frameworks ni capas externas.
     *
     * Condiciones:
     * - Clases de producción en com.hotelsearch.domain
     *
     * Se espera:
     * - Solo dependencias hacia java.. y el propio dominio
     */
    @Test
    void domain_DependsOnlyOnJavaAndDomain() {
        // given
        ArchRule rule = classes().that().resideInAPackage("com.hotelsearch.domain..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage("java..", "com.hotelsearch.domain..");

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "El dominio solo puede depender de java y de sí mismo\n" + result.getFailureReport());
    }

    /**
     * Verifica que la aplicación no depende de infraestructura ni de
     * frameworks.
     *
     * Condiciones:
     * - Clases de producción en com.hotelsearch.application
     *
     * Se espera:
     * - Solo dependencias hacia java.., el dominio y la propia aplicación
     */
    @Test
    void application_DependsOnlyOnJavaDomainAndApplication() {
        // given
        ArchRule rule = classes().that().resideInAPackage("com.hotelsearch.application..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage("java..", "com.hotelsearch.domain..", "com.hotelsearch.application..");

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "La aplicación solo puede depender de java, del dominio y de sí misma\n"
                        + result.getFailureReport());
    }

    /**
     * Verifica que los puertos no conocen a quienes los implementan o usan
     * dentro de la aplicación.
     *
     * Condiciones:
     * - Clases en ..application.port..
     *
     * Se espera:
     * - Ninguna dependencia hacia ..application.usecase..
     */
    @Test
    void ports_DoNotDependOnUseCases() {
        // given
        ArchRule rule = noClasses().that().resideInAPackage("..application.port..")
                .should().dependOnClassesThat().resideInAPackage("..application.usecase..");

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "Los puertos no pueden depender de los casos de uso\n" + result.getFailureReport());
    }

    /**
     * Verifica que los puertos son contratos. Las excepciones son la única
     * clase concreta admitida, porque forman parte de la firma del contrato.
     *
     * Condiciones:
     * - Clases en ..application.port.. que no son Throwable
     *
     * Se espera:
     * - Todas son interfaces
     */
    @Test
    void ports_AreInterfacesExceptExceptions() {
        // given
        ArchRule rule = classes().that().resideInAPackage("..application.port..")
                .and().areNotAssignableTo(Throwable.class)
                .should().beInterfaces();

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "Los puertos deberían ser interfaces, salvo las excepciones del contrato\n"
                        + result.getFailureReport());
    }

    /**
     * Verifica que cada caso de uso se expone a través de un puerto de
     * entrada, para que los adaptadores dependan del contrato y no de la
     * implementación.
     *
     * Condiciones:
     * - Clases en ..application.usecase.. que no son Throwable
     *
     * Se espera:
     * - Todas implementan una interfaz de ..application.port.in..
     */
    @Test
    void useCases_ImplementInputPorts() {
        // given
        ArchRule rule = classes().that().resideInAPackage("..application.usecase..")
                .and().areNotAssignableTo(Throwable.class)
                .should().beAssignableTo(resideInAPackage("..application.port.in..").and(INTERFACES)
                        .as("an interface in ..application.port.in.."));

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "Cada caso de uso debería implementar un puerto de entrada\n" + result.getFailureReport());
    }

    /**
     * Verifica que los adaptadores usan los puertos de entrada y no las
     * implementaciones. Solo infrastructure.config puede instanciar los casos
     * de uso.
     *
     * Condiciones:
     * - Clases en ..infrastructure.input.. o ..infrastructure.output..
     *
     * Se espera:
     * - Ninguna dependencia hacia ..application.usecase..
     */
    @Test
    void adapters_DoNotDependOnUseCaseImplementations() {
        // given
        ArchRule rule = noClasses().that()
                .resideInAnyPackage("..infrastructure.input..", "..infrastructure.output..")
                .should().dependOnClassesThat().resideInAPackage("..application.usecase..")
                .allowEmptyShould(true);

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "Los adaptadores deberían depender de los puertos, no de los casos de uso\n"
                        + result.getFailureReport());
    }

    /**
     * Verifica que los adaptadores de entrada y de salida no se conocen entre
     * sí; se comunican únicamente a través de la aplicación.
     *
     * Condiciones:
     * - Clases en ..infrastructure.input.. y ..infrastructure.output..
     *
     * Se espera:
     * - Input no depende de output
     * - Output no depende de input
     */
    @Test
    void adapters_InputAndOutputAreIndependent() {
        // given
        ArchRule inputRule = noClasses().that().resideInAPackage("..infrastructure.input..")
                .should().dependOnClassesThat().resideInAPackage("..infrastructure.output..")
                .allowEmptyShould(true);
        ArchRule outputRule = noClasses().that().resideInAPackage("..infrastructure.output..")
                .should().dependOnClassesThat().resideInAPackage("..infrastructure.input..")
                .allowEmptyShould(true);

        // when
        EvaluationResult inputResult = inputRule.evaluate(CLASSES);
        EvaluationResult outputResult = outputRule.evaluate(CLASSES);

        // then
        assertAll(
                () -> assertFalse(inputResult.hasViolation(),
                        "Los adaptadores de entrada no pueden depender de los de salida\n"
                                + inputResult.getFailureReport()),
                () -> assertFalse(outputResult.hasViolation(),
                        "Los adaptadores de salida no pueden depender de los de entrada\n"
                                + outputResult.getFailureReport()));
    }

    /**
     * Verifica que no hay ciclos entre paquetes, que son la forma habitual en
     * que una frontera se rompe sin que ninguna regla puntual lo detecte.
     *
     * Condiciones:
     * - Un slice por cada paquete bajo com.hotelsearch
     *
     * Se espera:
     * - Ningún ciclo de dependencias entre slices
     */
    @Test
    void packages_AreFreeOfCycles() {
        // given
        ArchRule rule = slices().matching("com.hotelsearch.(**)").should().beFreeOfCycles();

        // when
        EvaluationResult result = rule.evaluate(CLASSES);

        // then
        assertFalse(result.hasViolation(),
                "Los paquetes no deberían tener dependencias cíclicas\n" + result.getFailureReport());
    }
}
