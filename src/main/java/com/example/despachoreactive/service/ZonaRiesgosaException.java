package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

/** El score de riesgo devuelto por el simulador supera lo permitido: se rechaza el despacho con 422 antes de reservar cupo. */
public class ZonaRiesgosaException extends DomainException {
    public ZonaRiesgosaException(int scoreRiesgo) {
        super("Zona riesgosa: score " + scoreRiesgo + " supera el umbral permitido", HttpStatus.UNPROCESSABLE_ENTITY);
    }
}
