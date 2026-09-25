package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

/** El vehiculo no tiene cupo disponible para el peso solicitado en este momento: 409 (es carrera esperable, no un bug). */
public class CupoInsuficienteException extends DomainException {
    public CupoInsuficienteException(Long vehiculoId, int pesoKg) {
        super("Cupo insuficiente para vehiculo " + vehiculoId + " y peso " + pesoKg, HttpStatus.CONFLICT);
    }
}
