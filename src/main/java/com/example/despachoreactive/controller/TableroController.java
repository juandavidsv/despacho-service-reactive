package com.example.despachoreactive.controller;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * El tablero operativo global: todos los clientes que se conecten aca ven
 * exactamente el mismo stream (hot, compartido), no una copia independiente
 * cada uno. publish().refCount(1) hace que el bus se conecte una sola vez,
 * sin importar cuantos suscriptores lleguen despues.
 *
 * onBackpressureLatest: si un cliente lento no alcanza a consumir al ritmo
 * que llegan los eventos, preferimos que reciba el ultimo estado disponible
 * antes que acumular una cola creciente en memoria (el tablero muestra
 * "que esta pasando ahora", no un historial completo).
 *
 * distinctUntilChanged: evita mandar dos veces seguidas la misma combinacion
 * despacho+estado si por algun motivo se publicara un evento repetido.
 */
@RestController
@RequestMapping("/api/ops")
public class TableroController {
    private final EventBus bus;

    public TableroController(EventBus bus) {
        this.bus = bus;
    }

    @GetMapping(value = "/tablero", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<DespachoEvent> tablero() {
        return bus.eventos().publish().refCount(1).onBackpressureLatest()
                .distinctUntilChanged(evento -> evento.despachoId() + ":" + evento.estado());
    }
}
