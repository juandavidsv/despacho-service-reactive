package com.example.despachoreactive;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;

class FluxMergeAndLifecycleTest {
    @Test
    void mergeCombinaEventosActivosYTerminales() {
        EventBus bus = new EventBus();
        DespachoEvent activo = new DespachoEvent(10L, "ASIGNADO", "activo", "trace", Instant.now());
        DespachoEvent terminal = new DespachoEvent(11L, "EXPIRADO", "terminal", "trace", Instant.now());

        StepVerifier.create(bus.eventosCombinados().take(2))
                .then(() -> {
                    bus.publicar(activo);
                    bus.publicar(terminal);
                })
                .expectNext(activo)
                .expectNext(terminal)
                .verifyComplete();
    }

    @Test
    void doOnCancelYDoFinallySePuedenObservarEnFlujoCorto() {
        StepVerifier.create(Flux.just(1, 2, 3)
                .doOnCancel(() -> System.out.println("cancelado"))
                .doFinally(signal -> System.out.println("finalizado: " + signal))
                .take(2))
                .expectNext(1, 2)
                .verifyComplete();
    }
}
