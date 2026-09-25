package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class EstadoInvalidoException extends DomainException {
    public EstadoInvalidoException(Long despachoId, String estadoActual, String estadoEsperado) {
        super("Despacho " + despachoId + " en estado " + estadoActual + ", se esperaba " + estadoEsperado, HttpStatus.CONFLICT);
    }
}
