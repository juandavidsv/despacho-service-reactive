package com.example.despachoreactive.model;

/** Ciclo de vida de un despacho: nace RECIBIDO, se le asigna vehiculo (ASIGNADO), viaja (EN_RUTA) y termina en ENTREGADO, RECHAZADO o EXPIRADO. */
public enum EstadoDespacho {
    RECIBIDO, ASIGNADO, EN_RUTA, ENTREGADO, RECHAZADO, EXPIRADO
}
