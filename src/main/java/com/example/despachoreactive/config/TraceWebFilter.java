package com.example.despachoreactive.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.WebFilter;

import java.util.UUID;

/**
 * Este filtro es la puerta de entrada del "trazaId": lo lee del header
 * X-Traza-Id (o inventa uno nuevo si el cliente no mando ninguno) y lo
 * escribe en el Reactor Context con contextWrite, no en un campo ni en un
 * parametro. Gracias a eso, cualquier servicio mas adelante en la cadena
 * (DespachoService, el manejador de errores, etc.) puede leerlo con
 * deferContextual sin que nadie se lo tenga que pasar a mano.
 */
@Configuration
public class TraceWebFilter {
    private static final Logger log = LoggerFactory.getLogger(TraceWebFilter.class);
    public static final String KEY = "trazaId";

    @Bean
    public WebFilter traceFilter() {
        return (exchange, chain) -> {
            String trace = exchange.getRequest().getHeaders().getFirst("X-Traza-Id");
            String value = trace == null || trace.isBlank() ? UUID.randomUUID().toString() : trace;
            // Devolvemos el mismo trazaId en la respuesta: sirve para que quien llamo pueda
            // correlacionar su request con los logs del servidor, aunque no haya mandado uno.
            exchange.getResponse().getHeaders().add("X-Traza-Id", value);
            log.info("Inicio request {} {} trazaId={}", exchange.getRequest().getMethod(), exchange.getRequest().getPath(), value);
            return chain.filter(exchange).contextWrite(context -> context.put(KEY, value));
        };
    }
}
