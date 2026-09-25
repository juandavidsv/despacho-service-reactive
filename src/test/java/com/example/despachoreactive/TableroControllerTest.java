package com.example.despachoreactive;

import com.example.despachoreactive.controller.TableroController;
import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;

class TableroControllerTest {
    @Test
    void tableroComparteEventosDelBus() {
        EventBus bus = new EventBus();
        TableroController controller = new TableroController(bus);
        DespachoEvent evento = new DespachoEvent(4L, "ASIGNADO", "ok", "trace", Instant.now());

        StepVerifier.create(controller.tablero().take(1))
                .then(() -> bus.publicar(evento))
                .expectNext(evento)
                .verifyComplete();
    }

    @Test
    void tableroDescartaNotificacionesRepetidasDelMismoDespachoYEstado() {
        EventBus bus = new EventBus();
        TableroController controller = new TableroController(bus);
        DespachoEvent primero = new DespachoEvent(4L, "ASIGNADO", "ok", "trace", Instant.now());
        DespachoEvent duplicado = new DespachoEvent(4L, "ASIGNADO", "reintento", "trace", Instant.now().plusSeconds(1));
        DespachoEvent otroEstado = new DespachoEvent(4L, "EN_RUTA", "confirmado", "trace", Instant.now().plusSeconds(2));

        StepVerifier.create(controller.tablero().take(2))
                .then(() -> {
                    bus.publicar(primero);
                    bus.publicar(duplicado);
                    bus.publicar(otroEstado);
                })
                .expectNext(primero)
                .expectNext(otroEstado)
                .verifyComplete();
    }
}
