package com.example.despachoreactive.service;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Aca vive toda la resiliencia frente a los tres servicios externos simulados
 * (tarifa, riesgo y ventana de entrega). La idea de fondo: nunca dejar que una
 * falla o una lentitud de un tercero tumbe la creacion de un despacho. Cada
 * metodo decide su propia estrategia porque cada servicio falla distinto.
 */
@Service
public class ServiciosExternosClient {
    private final WebClient client;

    public ServiciosExternosClient(WebClient externalWebClient) {
        this.client = externalWebClient;
    }

    /**
     * La tarifa es intermitente: a veces responde 5xx sin razon aparente.
     * Reintentamos con backoff (3 intentos, 200ms de base) solo si el error
     * parece transitorio; si el simulador sigue fallando, no nos quedamos
     * esperando para siempre y devolvemos la tarifa de catalogo que ya
     * calculamos localmente.
     */
    public Mono<BigDecimal> tarifa(String ciudad, int peso, BigDecimal fallback) {
        return client.get().uri(uri -> uri.path("/external/pricing").queryParam("ciudad", ciudad).queryParam("peso", peso).build()).retrieve().bodyToMono(BigDecimal.class)
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)).filter(this::transitorio))
                .onErrorReturn(fallback);
    }

    /**
     * El scoring de riesgo puede colgarse (simula un servicio lento de verdad).
     * En vez de reintentar, le ponemos un limite de 800ms: si no contesta a
     * tiempo, asumimos un riesgo por defecto en vez de bloquear el despacho.
     */
    public Mono<Integer> riesgo(String ciudad) {
        return client.get().uri(uri -> uri.path("/external/risk").queryParam("ciudad", ciudad).build()).retrieve().bodyToMono(Integer.class)
                .timeout(Duration.ofMillis(800)).onErrorReturn(30);
    }

    /**
     * La ventana de clima/entrega es lenta pero estable, asi que no vale la
     * pena consultarla en cada request: la cacheamos 10 minutos. El
     * onErrorReturn es igual de importante que el cache: si el simulador
     * esta en modo fallo, igual queremos poder seguir creando despachos con
     * una ventana estandar en vez de tumbar todo el Mono.zip.
     */
    public Mono<String> ventana(String ciudad) {
        return client.get().uri(uri -> uri.path("/external/window").queryParam("ciudad", ciudad).build()).retrieve().bodyToMono(String.class)
                .onErrorReturn("VENTANA_ESTANDAR")
                .cache(Duration.ofMinutes(10));
    }

    /** Solo reintentamos ante 5xx o errores de red; un 4xx es un error nuestro, no vale la pena insistir. */
    private boolean transitorio(Throwable error) {
        return !(error instanceof org.springframework.web.reactive.function.client.WebClientResponseException response) || response.getStatusCode().is5xxServerError();
    }
}
