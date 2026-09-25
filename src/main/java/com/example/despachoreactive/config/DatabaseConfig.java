package com.example.despachoreactive.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Registra el TransactionalOperator que usa DespachoService para envolver
 * SOLO el guardado del despacho y sus paquetes (no las llamadas externas ni
 * las reservas de cupo, que van por fuera a proposito). Nada de
 * @Transactional imperativo: en un mundo reactivo eso no funciona igual y
 * esta prohibido por las reglas del taller.
 */
@Configuration
public class DatabaseConfig {
    @Bean
    ReactiveTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }

    @Bean
    TransactionalOperator transactionalOperator(ReactiveTransactionManager transactionManager) {
        return TransactionalOperator.create(transactionManager);
    }
}
