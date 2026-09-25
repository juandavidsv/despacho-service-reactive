package com.example.despachoreactive.service;

import com.example.despachoreactive.config.TraceWebFilter;
import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.dto.DespachoRequest;
import com.example.despachoreactive.dto.DespachoResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Este es el corazon del taller: toda la logica de crear, reservar, confirmar
 * y consultar un despacho vive aca. Vale la pena leerlo pensando en la
 * historia completa de un envio, no operador por operador:
 *
 * 1. Llega la solicitud -> se consultan tarifa/clima/riesgo en paralelo.
 * 2. Si el riesgo es muy alto, se corta ahi mismo (nunca se llega a reservar cupo).
 * 3. Si el riesgo es aceptable, se reserva cupo paquete por paquete. Si un
 *    paquete falla a mitad de camino, se devuelve lo que ya se habia tomado
 *    (una compensacion manual, al estilo saga, porque estas reservas NO estan
 *    dentro de una transaccion de base de datos).
 * 4. Recien ahi se abre una transaccion real para guardar el despacho y sus
 *    paquetes de forma atomica.
 * 5. Se avisa por el bus de eventos que el despacho quedo asignado.
 */
@Service
public class DespachoService {
    private static final Logger log = LoggerFactory.getLogger(DespachoService.class);

    // Guardamos el despacho recien creado en estado RECIBIDO/ASIGNADO segun el flujo.
    private static final String INSERT = "INSERT INTO despacho (cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, idem_key, expira_en) VALUES (:clienteId, :ciudad, 'RECIBIDO', :tarifa, :total, :score, :trazaId, :idemKey, :expiraEn) RETURNING id, cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, creado_en, expira_en";

    // El truco de la atomicidad: la condicion "cupo_kg >= :peso" va en el WHERE, no en un IF aparte.
    // Si dos requests compiten por el mismo vehiculo, Postgres solo deja pasar al que realmente alcanza cupo;
    // el otro simplemente actualiza 0 filas (y ahi lo detectamos con el RETURNING vacio).
    private static final String RESERVE = "UPDATE vehiculo SET cupo_kg = cupo_kg - :peso, reservado_kg = reservado_kg + :peso WHERE id = :vehiculoId AND cupo_kg >= :peso RETURNING id";

    // La contraparte de RESERVE: se usa tanto en la compensacion (saga) como cuando expira una asignacion.
    private static final String RELEASE = "UPDATE vehiculo SET cupo_kg = cupo_kg + :peso, reservado_kg = GREATEST(reservado_kg - :peso, 0) WHERE id = :vehiculoId";

    private final DatabaseClient db;
    private final TransactionalOperator tx;
    private final EventBus bus;
    private final ServiciosExternosClient externos;

    public DespachoService(DatabaseClient db, TransactionalOperator tx, EventBus bus, ServiciosExternosClient externos) {
        this.db = db;
        this.tx = tx;
        this.bus = bus;
        this.externos = externos;
    }

    /**
     * Punto de entrada del caso de uso completo. Ojo con dos detalles de diseño:
     * - El trazaId NO llega como parametro del metodo: se lee del Reactor Context
     *   (lo puso el TraceWebFilter al entrar el request). Asi evitamos ensuciar
     *   la firma de todos los metodos con un parametro que no es parte del negocio.
     * - Las tres consultas externas se disparan con Mono.zip, es decir en paralelo:
     *   el tiempo total es el del mas lento de los tres, no la suma de los tres.
     */
    public Mono<DespachoResponse> crear(DespachoRequest request, String idemKey) {
        int peso = request.paquetes().stream().mapToInt(DespachoRequest.PaqueteRequest::pesoKg).sum();
        BigDecimal fallback = BigDecimal.valueOf(10000L + peso * 10L);
        return Mono.deferContextual(context -> {
            String trazaId = context.getOrDefault(TraceWebFilter.KEY, "n/a");
            log.info("Crear despacho cliente={} ciudad={} trazaId={}", request.clienteId(), request.ciudad(), trazaId);
            return Mono.zip(externos.tarifa(request.ciudad(), peso, fallback), externos.ventana(request.ciudad()), externos.riesgo(request.ciudad()))
                    .flatMap(datos -> {
                        // Riesgo por encima del umbral: se rechaza antes de tocar cupo de vehiculos.
                        if (datos.getT3() > 80) return Mono.error(new ZonaRiesgosaException(datos.getT3()));
                        // Sin Idempotency-Key, siempre creamos un despacho nuevo.
                        // Con Idempotency-Key, primero buscamos si ya existe uno con esa clave
                        // (asi un reintento del cliente no genera un segundo despacho duplicado).
                        return idemKey == null || idemKey.isBlank() ? crearNuevo(request, trazaId, null, datos.getT1(), datos.getT3()) : buscarPorClave(idemKey).switchIfEmpty(Mono.defer(() -> crearNuevo(request, trazaId, idemKey, datos.getT1(), datos.getT3())));
                    });
        });
    }

    /**
     * Reserva cupo paquete por paquete y, si todo sale bien, persiste el
     * despacho en una unica transaccion. Si algo falla en el camino (un
     * paquete sin cupo, un error de base de datos al guardar), se liberan
     * las reservas que ya se habian tomado: esa es la "saga" de compensacion.
     */
    private Mono<DespachoResponse> crearNuevo(DespachoRequest request, String trazaId, String idemKey, BigDecimal tarifa, Integer scoreRiesgo) {
        Instant expira = Instant.now().plus(Duration.ofMinutes(15));
        List<DespachoRequest.PaqueteRequest> reservados = new ArrayList<>();
        Mono<DespachoResponse> persistencia = Flux.fromIterable(request.paquetes())
            // concatMap y no flatMap: las reservas deben ir en orden, una despues de otra,
            // para poder registrar con certeza cuales quedaron confirmadas antes de un posible fallo.
            .concatMap(paquete -> reservar(paquete).doOnNext(ignorado -> reservados.add(paquete)))
            .then(Mono.defer(() -> {
                var spec = db.sql(INSERT)
                        .bind("clienteId", request.clienteId()).bind("ciudad", request.ciudad())
                        .bind("tarifa", tarifa).bind("total", tarifa).bind("score", scoreRiesgo)
                        .bind("trazaId", trazaId).bind("expiraEn", expira);
                // R2DBC exige bindNull explicito cuando el valor es null; bind() a secas revienta.
                spec = idemKey == null ? spec.bindNull("idemKey", String.class) : spec.bind("idemKey", idemKey);
                // A partir de aca (y solo a partir de aca) hay una transaccion real: insertar el
                // despacho, sus paquetes y pasar a ASIGNADO ocurre todo junto o no ocurre nada.
                return tx.transactional(spec
                        .map((row, metadata) -> new DespachoResponse(row.get("id", Long.class), row.get("cliente_id", Long.class), row.get("ciudad", String.class), row.get("estado", String.class), row.get("tarifa", BigDecimal.class), row.get("total", BigDecimal.class), row.get("score_riesgo", Integer.class), row.get("traza_id", String.class), row.get("creado_en", Instant.class), row.get("expira_en", Instant.class), List.of()))
                        .one()
                        .flatMap(despacho -> guardarPaquetes(despacho.id(), request.paquetes()).then(cambiarEstado(despacho.id(), "ASIGNADO")).then(cargar(despacho.id()))));
            }))
            .doOnSuccess(resultado -> bus.publicar(new DespachoEvent(resultado.id(), resultado.estado(), "Despacho asignado", trazaId, Instant.now())))
            // Si algo de lo anterior fallo, primero devolvemos el cupo reservado y despues
            // dejamos que el error siga su curso hacia el manejador global.
            .onErrorResume(error -> liberar(reservados).then(Mono.error(error)));
        return persistencia;
    }

    /**
     * Intenta reservar un paquete puntual. Si el UPDATE no afecta ninguna fila
     * (el RETURNING viene vacio) puede ser por dos motivos bien distintos, y
     * el codigo de error que devolvemos depende de cual sea: vehiculo
     * inexistente (404) o cupo insuficiente (409).
     */
    private Mono<Long> reservar(DespachoRequest.PaqueteRequest paquete) {
        return db.sql(RESERVE).bind("vehiculoId", paquete.vehiculoId()).bind("peso", paquete.pesoKg()).map((row, metadata) -> row.get("id", Long.class)).one()
                .switchIfEmpty(existeVehiculo(paquete.vehiculoId())
                        .flatMap(existe -> existe
                                ? Mono.<Long>error(new CupoInsuficienteException(paquete.vehiculoId(), paquete.pesoKg()))
                                : Mono.<Long>error(new VehiculoNoExisteException(paquete.vehiculoId()))));
    }

    private Mono<Boolean> existeVehiculo(Long vehiculoId) {
        return db.sql("SELECT id FROM vehiculo WHERE id = :id").bind("id", vehiculoId).map((r, m) -> r.get("id", Long.class)).one()
                .map(id -> true).defaultIfEmpty(false);
    }

    /** Devuelve el cupo tomado por una lista de paquetes. Se usa en la compensacion cuando algo falla. */
    private Mono<Void> liberar(List<DespachoRequest.PaqueteRequest> paquetes) {
        return Flux.fromIterable(paquetes).concatMap(p -> db.sql(RELEASE).bind("vehiculoId", p.vehiculoId()).bind("peso", p.pesoKg()).fetch().rowsUpdated()).then();
    }

    private Mono<Void> guardarPaquetes(Long despachoId, List<DespachoRequest.PaqueteRequest> paquetes) {
        return Flux.fromIterable(paquetes).concatMap(p -> db.sql("INSERT INTO paquete (despacho_id, vehiculo_id, peso_kg) VALUES (:despachoId, :vehiculoId, :peso)").bind("despachoId", despachoId).bind("vehiculoId", p.vehiculoId()).bind("peso", p.pesoKg()).fetch().rowsUpdated()).then();
    }

    public Mono<DespachoResponse> buscar(Long id) {
        return cargar(id).switchIfEmpty(Mono.error(new DespachoNoExisteException(id)));
    }

    /** Soporte de idempotencia: si el cliente reintenta con la misma clave, devolvemos el despacho ya creado. */
    private Mono<DespachoResponse> buscarPorClave(String clave) {
        return db.sql("SELECT id FROM despacho WHERE idem_key = :clave").bind("clave", clave).map((r, m) -> r.get("id", Long.class)).one().flatMap(this::cargar);
    }

    private Mono<DespachoResponse> cargar(Long id) {
        return db.sql("SELECT id, cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, creado_en, expira_en FROM despacho WHERE id = :id").bind("id", id).map((r, m) -> new DespachoResponse(r.get("id", Long.class), r.get("cliente_id", Long.class), r.get("ciudad", String.class), r.get("estado", String.class), r.get("tarifa", BigDecimal.class), r.get("total", BigDecimal.class), r.get("score_riesgo", Integer.class), r.get("traza_id", String.class), r.get("creado_en", Instant.class), r.get("expira_en", Instant.class), List.of())).one().flatMap(d -> db.sql("SELECT id, despacho_id, vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id ORDER BY id").bind("id", id).map((r, m) -> new DespachoResponse.PaqueteResponse(r.get("id", Long.class), r.get("despacho_id", Long.class), r.get("vehiculo_id", Long.class), r.get("peso_kg", Integer.class))).all().collectList().map(p -> new DespachoResponse(d.id(), d.clienteId(), d.ciudad(), d.estado(), d.tarifa(), d.total(), d.scoreRiesgo(), d.trazaId(), d.creadoEn(), d.expiraEn(), p)));
    }

    /**
     * El cliente confirma que recibio/acepto el despacho. Solo tiene sentido
     * si estaba ASIGNADO (si ya expiro o esta en otro estado, es un 409).
     * Al confirmar, el cupo pasa de "reservado" a "consumido de verdad":
     * ya no hace falta seguir cargando reservado_kg porque el vehiculo
     * efectivamente salio con esa carga.
     */
    public Mono<DespachoResponse> confirmar(Long id) {
        return buscar(id).flatMap(d -> {
            if (!"ASIGNADO".equals(d.estado())) return Mono.error(new EstadoInvalidoException(id, d.estado(), "ASIGNADO"));
                return db.sql("SELECT vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id").bind("id", id).map((r, m) -> new DespachoRequest.PaqueteRequest(r.get("vehiculo_id", Long.class), r.get("peso_kg", Integer.class))).all()
                    .concatMap(p -> db.sql("UPDATE vehiculo SET reservado_kg = GREATEST(reservado_kg - :peso, 0) WHERE id = :vehiculoId").bind("peso", p.pesoKg()).bind("vehiculoId", p.vehiculoId()).fetch().rowsUpdated()).then(cambiarEstado(id, "EN_RUTA")).then(cargar(id))
                    .doOnSuccess(resultado -> bus.publicar(new DespachoEvent(resultado.id(), resultado.estado(), "Despacho confirmado", resultado.trazaId(), Instant.now())));
        });
    }

    private Mono<Long> cambiarEstado(Long id, String estado) {
        return db.sql("UPDATE despacho SET estado = :estado WHERE id = :id").bind("estado", estado).bind("id", id).fetch().rowsUpdated();
    }
}
