plugins {
    java
    jacoco
    alias(libs.plugins.spring.boot)
}

group = "com.hotelsearch"
version = "0.0.1-SNAPSHOT"

// Los integration tests (Testcontainers) solo corren con -PintegrationTest=true.
val integrationTestEnabled = providers.gradleProperty("integrationTest")
    .map { it.toBoolean() }
    .getOrElse(false)

// Mínimo exigido para cada contador de JaCoCo.
val minimumCoverage = "0.80".toBigDecimal()

// Clases sin lógica propia que no aportan a la medición de cobertura.
val coverageExclusions = listOf("com/hotelsearch/HotelSearchApplication.class")

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(libs.versions.java.get())
    }
}

sourceSets {
    create("intTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

configurations {
    named("intTestImplementation") { extendsFrom(configurations.testImplementation.get()) }
    named("intTestRuntimeOnly") { extendsFrom(configurations.testRuntimeOnly.get()) }
}

dependencies {
    implementation(platform(libs.spring.boot.bom))
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.jdbc)
    implementation(libs.spring.boot.starter.flyway)
    implementation(libs.flyway.database.oracle)
    implementation(libs.spring.boot.starter.kafka)
    runtimeOnly(libs.ojdbc)

    testImplementation(platform(libs.spring.boot.bom))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.archunit)
    testRuntimeOnly(libs.junit.platform.launcher)

    "intTestImplementation"(libs.testcontainers)
    "intTestImplementation"(libs.testcontainers.junit.jupiter)
    "intTestImplementation"(libs.testcontainers.kafka)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Ejecuta los integration tests de src/intTest contra Kafka y Oracle reales."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs = sourceSets["intTest"].output.classesDirs
    classpath = sourceSets["intTest"].runtimeClasspath
    shouldRunAfter(tasks.test)
}

jacoco {
    toolVersion = libs.versions.jacoco.get()
}

// Tareas de test cuya ejecución alimenta la cobertura en este build.
val coverageTestTasks = if (integrationTestEnabled) listOf(tasks.test, integrationTest) else listOf(tasks.test)

tasks.withType<JacocoReportBase>().configureEach {
    dependsOn(coverageTestTasks)
    executionData.setFrom(coverageTestTasks.map { task ->
        task.map { it.extensions.getByType<JacocoTaskExtension>().destinationFile!! }
    })
    // Sin integration tests en src/intTest no se genera su archivo .exec.
    executionData.setFrom(executionData.filter { it.exists() })
    classDirectories.setFrom(sourceSets.main.get().output.classesDirs.asFileTree.matching {
        exclude(coverageExclusions)
    })
}

tasks.jacocoTestReport {
    reports {
        xml.required = true
        html.required = true
    }
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            listOf("LINE", "BRANCH").forEach { counterName ->
                limit {
                    counter = counterName
                    value = "COVEREDRATIO"
                    minimum = minimumCoverage
                }
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestReport, tasks.jacocoTestCoverageVerification)
    if (integrationTestEnabled) {
        dependsOn(integrationTest)
    }
}

// Solo se distribuye el jar ejecutable de Spring Boot.
tasks.jar {
    enabled = false
}
