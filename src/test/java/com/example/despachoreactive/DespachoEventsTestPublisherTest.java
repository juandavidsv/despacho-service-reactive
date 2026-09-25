package com.example.despachoreactive;

import com.example.despachoreactive.controller.DespachoController;
import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.DespachoService;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.Mockito.mock;

class DespachoEventsTestPublisherTest {

    @Test
    void streamDeUnDespachoSeCierraAlLlegarAEstadoTerminal() {
        EventBus bus = mock(EventBus.class);
        TestPublisher<DespachoEvent> publisher = TestPublisher.create();
        org.mockito.Mockito.when(bus.eventos()).thenReturn(publisher.flux());

        DespachoController controller = new DespachoController(mock(DespachoService.class), bus);

        DespachoEvent asignado = new DespachoEvent(5L, "ASIGNADO", "creado", "traza", Instant.now());
        DespachoEvent enRuta = new DespachoEvent(5L, "EN_RUTA", "confirmado", "traza", Instant.now());
        DespachoEvent entregado = new DespachoEvent(5L, "ENTREGADO", "cerrado", "traza", Instant.now());
        DespachoEvent otroDespacho = new DespachoEvent(99L, "ASIGNADO", "otro", "traza", Instant.now());

        StepVerifier.create(controller.eventos(5L))
                .then(() -> publisher.next(otroDespacho))
                .then(() -> publisher.next(asignado))
                .expectNext(asignado)
                .then(() -> publisher.next(enRuta))
                .expectNext(enRuta)
                .then(() -> publisher.next(entregado))
                .expectNext(entregado)
                .verifyComplete();
    }

    @Test
    void streamDeUnDespachoSeCancelaSiSeAgotaElTimeout() {
        EventBus bus = mock(EventBus.class);
        TestPublisher<DespachoEvent> publisher = TestPublisher.create();
        org.mockito.Mockito.when(bus.eventos()).thenReturn(publisher.flux());

        DespachoController controller = new DespachoController(mock(DespachoService.class), bus);

        StepVerifier.withVirtualTime(() -> controller.eventos(1L))
                .thenAwait(Duration.ofMinutes(16))
                .expectError()
                .verify();
    }
}
