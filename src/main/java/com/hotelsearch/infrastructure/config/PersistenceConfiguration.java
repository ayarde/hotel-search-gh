package com.hotelsearch.infrastructure.config;

import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.infrastructure.output.persistence.JdbcHotelSearchRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Beans del adaptador de persistencia Oracle. El {@link JdbcClient} lo provee
 * la autoconfiguración de Spring Boot sobre el DataSource de HikariCP.
 */
@Configuration(proxyBeanMethods = false)
public class PersistenceConfiguration {

    @Bean
    HotelSearchRepository hotelSearchRepository(JdbcClient jdbcClient) {
        return new JdbcHotelSearchRepository(jdbcClient);
    }
}
