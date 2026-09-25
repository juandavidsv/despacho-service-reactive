package com.example.despachoreactive.service;

import com.example.despachoreactive.dto.DespachoEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * El "corazon" de los eventos en vivo. Es un bus hot: no guarda historial,
 * solo retransmite lo que va pasando en el momento a quien este escuchando.
 * Tanto el SSE de un despacho puntual como el tablero global beben de aca.
 *
 * Que sea hot (y no cold) es una decision a proposito: si el evento se
 * generara de nuevo por cada suscriptor, tendriamos que volver a ejecutar la
 * logica de negocio (o duplicar consultas) por cada cliente conectado, y eso
 * no tiene sentido para algo que representa "lo que esta pasando ahora".
 */
@Component
public class EventBus {
    private final Sinks.Many<DespachoEvent> eventos = Sinks.many().multicast().directBestEffort();

    /** Empuja un evento al bus. Si nadie esta escuchando en ese instante, el evento simplemente se pierde. */
    public void publicar(DespachoEvent evento) {
        eventos.emitNext(evento, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    /** El stream crudo, tal como llega. Quien se suscribe solo ve eventos futuros, nunca los pasados. */
    public Flux<DespachoEvent> eventos() {
        return eventos.asFlux();
    }

    /**
     * Ejemplo didactico de Flux.merge: partimos el mismo bus en dos flujos con
     * distinto significado (estados "en curso" vs estados "terminales") y los
     * volvemos a unir en un solo stream. Sirve para mostrar que se pueden
     * combinar fuentes reactivas sin perder el orden de llegada real.
     */
    public Flux<DespachoEvent> eventosCombinados() {
        Flux<DespachoEvent> activos = eventos().filter(evento -> "ASIGNADO".equals(evento.estado()) || "EN_RUTA".equals(evento.estado()));
        Flux<DespachoEvent> terminales = eventos().filter(evento -> "ENTREGADO".equals(evento.estado()) || "RECHAZADO".equals(evento.estado()) || "EXPIRADO".equals(evento.estado()));
        return Flux.merge(activos, terminales);
    }
}
