package com.example.despachoreactive.dto;

import java.time.Instant;

/** Notificacion que viaja por el EventBus (hot) hacia el tablero y hacia el SSE de un despacho puntual. */
public record DespachoEvent(Long despachoId, String estado, String mensaje, String trazaId, Instant instante) {}
