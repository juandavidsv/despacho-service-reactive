package com.example.despachoreactive.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/** Representa un vehiculo tanto para crearlo/actualizarlo como para devolverlo en las respuestas. */
public record VehiculoRequest(@NotNull Long id, @NotBlank String placa, @NotBlank String ciudad, @PositiveOrZero int cupoKg) {}
