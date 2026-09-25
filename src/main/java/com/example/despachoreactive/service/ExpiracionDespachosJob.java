package com.example.despachoreactive.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Job de fondo que cada cierto intervalo (30s por defecto) revisa que
 * despachos ASIGNADOS ya vencieron su ventana de 15 minutos, les devuelve el
 * cupo a los vehiculos y los marca como EXPIRADO. Es una suscripcion de
 * larga vida: se arranca cuando la app termina de iniciar y se cierra
 * ordenadamente cuando el bean se destruye (Disposable + @PreDestroy).
 */
@Component
public class ExpiracionDespachosJob {
    private static final Logger log = LoggerFactory.getLogger(ExpiracionDespachosJob.class);
    private final DatabaseClient db;
    private final EventBus bus;
    private final Duration intervalo;
    private final Clock reloj;
    private final Scheduler scheduler;
    private Disposable suscripcion;

    @Autowired
    public ExpiracionDespachosJob(DatabaseClient db, EventBus bus) {
        this(db, bus, Duration.ofSeconds(30), Clock.systemUTC(), Schedulers.parallel());
    }

    // Constructor secundario pensado para tests: permite inyectar un reloj y un scheduler controlados
    // (por ejemplo, un VirtualTimeScheduler) sin depender de que pase tiempo real.
    public ExpiracionDespachosJob(DatabaseClient db, EventBus bus, Duration intervalo, Clock reloj, Scheduler scheduler) {
        this.db = db;
        this.bus = bus;
        this.intervalo = intervalo;
        this.reloj = reloj;
        this.scheduler = scheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void iniciar() {
        suscripcion = Flux.interval(intervalo, scheduler)
                // Si un ciclo todavia esta corriendo cuando toca el siguiente tick, lo descartamos
                // en vez de acumular tics en cola: no tiene sentido "ponerse al dia" corriendo el
                // job varias veces seguidas, solo nos interesa el estado actual de la base.
                .onBackpressureDrop()
                .concatMap(tick -> expirar().onErrorResume(error -> {
                    log.warn("Error en expiración de despachos", error);
                    return Mono.empty();
                }))
                .doFinally(signal -> log.debug("Ciclo de vida del job terminado: {}", signal))
                .subscribe();
    }

    /** Busca despachos ASIGNADOS vencidos, libera su cupo y los marca EXPIRADO, uno por uno. */
    Mono<Long> expirar() {
        return db.sql("SELECT id FROM despacho WHERE estado = 'ASIGNADO' AND expira_en < :ahora")
                .bind("ahora", Instant.now(reloj))
                .map((r, m) -> r.get("id", Long.class))
                .all()
                .concatMap(id -> db.sql("SELECT vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id")
                        .bind("id", id)
                        .map((r, m) -> new long[]{r.get("vehiculo_id", Long.class), r.get("peso_kg", Integer.class)})
                        .all()
                        .concatMap(p -> db.sql("UPDATE vehiculo SET cupo_kg = cupo_kg + :peso, reservado_kg = GREATEST(reservado_kg - :peso, 0) WHERE id = :vehiculoId")
                                .bind("peso", p[1])
                                .bind("vehiculoId", p[0])
                                .fetch()
                                .rowsUpdated())
                        .then(db.sql("UPDATE despacho SET estado = 'EXPIRADO', expira_en = NULL WHERE id = :id")
                                .bind("id", id)
                                .fetch()
                                .rowsUpdated())
                        .doOnSuccess(n -> bus.publicar(new com.example.despachoreactive.dto.DespachoEvent(id, "EXPIRADO", "Despacho expirado", "job", Instant.now(reloj))))
                        .doOnCancel(() -> log.debug("Cancelada expiración del despacho {}", id))
                        .thenReturn(1L))
                .count();
    }

    /** Cierra la suscripcion de fondo de forma ordenada cuando el contexto de Spring se apaga. */
    @PreDestroy
    public void detener() {
        if (suscripcion != null) suscripcion.dispose();
    }
}
