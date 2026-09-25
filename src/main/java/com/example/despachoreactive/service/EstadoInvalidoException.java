package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

/** Se intento avanzar un despacho a un estado que no le corresponde desde el estado actual: 409. */
public class EstadoInvalidoException extends DomainException {
    public EstadoInvalidoException(Long despachoId, String estadoActual, String estadoEsperado) {
        super("Despacho " + despachoId + " en estado " + estadoActual + ", se esperaba " + estadoEsperado, HttpStatus.CONFLICT);
    }
}
