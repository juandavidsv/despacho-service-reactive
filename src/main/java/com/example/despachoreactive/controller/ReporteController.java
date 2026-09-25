package com.example.despachoreactive.controller;

import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/reports")
public class ReporteController {
    private final DatabaseClient db;

    public ReporteController(DatabaseClient db) {
        this.db = db;
    }

    private record FilaDespacho(String ciudad, long kilos, BigDecimal valor) {}

    private record TotalCiudad(String ciudad, long kilos, BigDecimal valor) {
        static TotalCiudad vacio(String ciudad) {
            return new TotalCiudad(ciudad, 0L, BigDecimal.ZERO);
        }

        TotalCiudad sumar(FilaDespacho fila) {
            return new TotalCiudad(ciudad, kilos + fila.kilos(), valor.add(fila.valor()));
        }

        Map<String, Object> aMapa() {
            return Map.of("ciudad", ciudad, "kilos", kilos, "valor", valor);
        }
    }

    /**
     * Trae una fila por despacho (sin agregar en SQL) para poder demostrar la agregacion
     * reactiva con groupBy/reduce. limitRate evita que miles de filas saturen la demanda.
     */
    private Flux<FilaDespacho> filasPorDespacho() {
        return db.sql("SELECT d.ciudad, COALESCE(SUM(p.peso_kg), 0) kilos, COALESCE(d.total, 0) valor FROM despacho d LEFT JOIN paquete p ON p.despacho_id = d.id GROUP BY d.id, d.ciudad, d.total")
                .map((r, m) -> new FilaDespacho(r.get("ciudad", String.class), r.get("kilos", Long.class), r.get("valor", BigDecimal.class)))
                .all()
                .limitRate(100);
    }

    @GetMapping("/ciudades")
    public Flux<Map<String, Object>> ciudades() {
        return filasPorDespacho()
                .groupBy(FilaDespacho::ciudad)
                .flatMap(grupo -> grupo.reduce(TotalCiudad.vacio(grupo.key()), TotalCiudad::sumar))
                .collectSortedList(Comparator.comparing(TotalCiudad::ciudad))
                .flatMapIterable(lista -> lista)
                .map(TotalCiudad::aMapa);
    }

    @GetMapping(value = "/ciudades/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Map<String, Object>> stream() {
        return ciudades().scan(new LinkedHashMap<String, Object>(), ReporteController::acumular);
    }

    public static Map<String, Object> acumular(Map<String, Object> acumulado, Map<String, Object> fila) {
        Map<String, Object> resultado = new LinkedHashMap<>(acumulado);
        String ciudad = (String) fila.get("ciudad");
        Map<String, Object> ciudades = new LinkedHashMap<>();
        Object existentes = resultado.get("ciudades");
        if (existentes instanceof Map<?, ?> mapa) {
            mapa.forEach((clave, valor) -> ciudades.put(String.valueOf(clave), valor));
        }
        ciudades.put(ciudad, fila);
        resultado.put("ciudades", ciudades);
        return resultado;
    }
}
