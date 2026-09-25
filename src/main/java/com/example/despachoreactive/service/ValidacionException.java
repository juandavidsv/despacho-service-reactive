package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class ValidacionException extends DomainException {
    public ValidacionException(String mensaje) {
        super(mensaje, HttpStatus.BAD_REQUEST);
    }
}
