package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class ZonaRiesgosaException extends DomainException {
    public ZonaRiesgosaException(int scoreRiesgo) {
        super("Zona riesgosa: score " + scoreRiesgo + " supera el umbral permitido", HttpStatus.UNPROCESSABLE_ENTITY);
    }
}
