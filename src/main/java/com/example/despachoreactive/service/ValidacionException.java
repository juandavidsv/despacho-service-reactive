package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

/** Datos de entrada mal formados (ej. placa invalida en la carga masiva): 400. */
public class ValidacionException extends DomainException {
    public ValidacionException(String mensaje) {
        super(mensaje, HttpStatus.BAD_REQUEST);
    }
}
