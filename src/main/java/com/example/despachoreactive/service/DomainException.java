package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

/** Base de todos los errores de negocio: cada uno trae su propio HttpStatus para que GlobalErrorHandler no tenga que adivinar. */
public class DomainException extends RuntimeException {
    private final HttpStatus status;

    public DomainException(String message, HttpStatus status) {
        super(message);
        this.status = status;
    }

    public HttpStatus status() {
        return status;
    }
}
