package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

/** No existe un despacho con ese id: 404. */
public class DespachoNoExisteException extends DomainException {
    public DespachoNoExisteException(Long id) {
        super("Despacho no existe: " + id, HttpStatus.NOT_FOUND);
    }
}
