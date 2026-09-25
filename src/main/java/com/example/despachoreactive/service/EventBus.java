package com.example.despachoreactive.service;

import com.example.despachoreactive.dto.DespachoEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class EventBus {
    private final Sinks.Many<DespachoEvent> eventos = Sinks.many().multicast().directBestEffort();

    public void publicar(DespachoEvent evento) {
        eventos.emitNext(evento, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    public Flux<DespachoEvent> eventos() {
        return eventos.asFlux();
    }

    public Flux<DespachoEvent> eventosCombinados() {
        Flux<DespachoEvent> activos = eventos().filter(evento -> "ASIGNADO".equals(evento.estado()) || "EN_RUTA".equals(evento.estado()));
        Flux<DespachoEvent> terminales = eventos().filter(evento -> "ENTREGADO".equals(evento.estado()) || "RECHAZADO".equals(evento.estado()) || "EXPIRADO".equals(evento.estado()));
        return Flux.merge(activos, terminales);
    }
}
