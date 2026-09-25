# despacho-service-reactive

**Integrantes del equipo:** Juan David Sanchez, Luis Hernan Torres, William Alexander Alarcon.

Este proyecto simula el backend de una operadora logistica: recibe solicitudes de despacho, reserva cupo en los vehiculos disponibles, consulta tarifa/clima/riesgo de forma simulada y va contando la historia completa de un envio hasta que se entrega (o se cae, o expira).

Esta hecho con Spring WebFlux, Spring Data R2DBC y Project Reactor, y la idea central del taller es que **nada bloquee un hilo**: ni la base de datos, ni las llamadas a servicios externos, ni el paso del tiempo.

## Que hace, en criollo

- Recibe un despacho, lo valida y lo guarda en estado `RECIBIDO`.
- Va paquete por paquete reservando cupo en el vehiculo correspondiente; si uno falla a mitad de camino, devuelve el cupo que ya habia tomado (como una reversion manual, sin usar transacciones imperativas).
- Mientras tanto, en paralelo, pregunta por tarifa, clima y riesgo a unos servicios simulados dentro de la misma app (para poder provocar fallas y probar reintentos, timeouts y caches).
- Si el riesgo es muy alto, rechaza el despacho y libera lo reservado.
- Si todo sale bien, guarda el despacho de forma transaccional y lo deja `ASIGNADO` con una expiracion de 15 minutos.
- El cliente puede confirmarlo (`EN_RUTA`) o dejarlo expirar; un job de fondo revisa cada 30 segundos y libera lo que ya vencio.
- Todo lo que pasa se transmite en vivo por SSE: hay un canal por despacho y un "tablero" compartido para ver todo a la vez.
- Tambien hay una carga masiva de vehiculos por NDJSON y un reporte de kilos/valor por ciudad que se puede consultar como JSON o como stream en vivo.
- Un identificador de traza (`X-Traza-Id`) viaja con cada request sin pasarse nunca como parametro explicito: vive en el Reactor Context.

## Stack y versiones

| Componente | Version o configuracion |
|---|---|
| Java de compilacion | 21 |
| JDK validado localmente | 25.0.4+7 |
| Spring Boot | 4.1.1 |
| Spring WebFlux | Gestionado por Spring Boot |
| Project Reactor | Gestionado por Spring Boot |
| Spring Data R2DBC | Gestionado por Spring Boot |
| PostgreSQL | 15 |
| Driver R2DBC | `org.postgresql:r2dbc-postgresql` |
| Build | Gradle Wrapper 9.1.0 |
| Tests | JUnit 5, Reactor Test, Spring WebFlux Test |

El proyecto usa APIs compatibles con Java 21 y se compila localmente con JDK 25. No se requieren APIs exclusivas de Java 25.

## Como esta armado por dentro

```text
HTTP/WebFlux
		|
		+-- Controllers (la puerta de entrada HTTP/SSE/NDJSON)
		|     +-- DespachoController
		|     +-- VehiculoController
		|     +-- ReporteController
		|     +-- TableroController
		|     +-- SimuladorController
		|
		+-- Servicios reactivos (donde vive la logica de negocio)
		|     +-- DespachoService
		|     +-- ServiciosExternosClient
		|     +-- ExpiracionDespachosJob
		|     +-- EventBus
		|
		+-- DatabaseClient / R2DBC
					PostgreSQL (local)
```

Un detalle importante del diseño: **la reserva de cupo y el guardado final no ocurren dentro de la misma transaccion**. La razon es simple, reservar cupo es una serie de `UPDATE ... RETURNING` independientes (uno por paquete), y si uno falla a mitad de camino hay que deshacer manualmente lo que ya se tomo, como una mini-saga. Una vez que todas las reservas quedan firmes, ahi si se abre una transaccion real (`TransactionalOperator`) solo para guardar el despacho y sus paquetes.

Tambien las consultas a los servicios externos (tarifa, clima, riesgo) se disparan en paralelo con `Mono.zip` **antes** de tocar la base de datos, para no dejar una transaccion abierta esperando una llamada HTTP lenta.

## Modelo de negocio

### Vehiculo

| Campo | Descripcion |
|---|---|
| `id` | Identificador del vehiculo |
| `placa` | Placa unica, maximo 10 caracteres |
| `ciudad` | Ciudad operativa |
| `cupo_kg` | Capacidad disponible |
| `reservado_kg` | Capacidad comprometida por despachos asignados |

Reglas:

- `cupo_kg` nunca puede ser negativo.
- `reservado_kg` nunca puede ser negativo.
- Una reserva solo descuenta capacidad si `cupo_kg >= peso`.
- Confirmar un despacho libera su cantidad de `reservado_kg`; el cupo ya fue descontado al reservar.
- Expirar un despacho devuelve el peso a `cupo_kg` y reduce `reservado_kg`.

### Despacho

Estados disponibles:

```text
RECIBIDO -> ASIGNADO -> EN_RUTA -> ENTREGADO
												 |
												 +-- EXPIRADO

RECIBIDO/ASIGNADO -> RECHAZADO
```

Estados terminales para SSE: `ENTREGADO`, `RECHAZADO` y `EXPIRADO`.

Al crear un despacho:

- Se valida el cliente, la ciudad y la lista de paquetes.
- Se calcula el peso total.
- Se consultan tarifa, ventana y riesgo.
- Un riesgo superior a `80` se rechaza con HTTP `422`.
- Se reserva capacidad paquete por paquete usando `concatMap`.
- Se asigna una expiracion de 15 minutos.
- Se publica un evento `ASIGNADO` cuando la persistencia termina correctamente.

La clave de idempotencia se almacena en `despacho.idem_key`. Una solicitud repetida busca el despacho existente y devuelve su representacion.

### Paquete

Cada paquete pertenece a un despacho y referencia un vehiculo. El peso debe ser mayor que cero.

## Base de datos

El esquema se encuentra en `src/main/resources/schema.sql` y crea:

- `vehiculo`
- `despacho`
- `paquete`
- Indices para expiracion y consulta de paquetes.

La inicializacion SQL esta configurada con `spring.sql.init.mode=always`. El esquema usa sentencias idempotentes y agrega tres vehiculos iniciales:

```text
1 - ABC123 - BOG - 500 kg
2 - XYZ987 - MDE - 200 kg
3 - JKL456 - CLO - 800 kg
```

## Configuracion

Configuracion predeterminada en `src/main/resources/application.yml`:

| Propiedad | Valor | Descripcion |
|---|---:|---|
| `spring.r2dbc.url` | `r2dbc:postgresql://localhost:5432/despachodb` | Conexion reactiva |
| `spring.r2dbc.username` | `postgres` | Usuario de base |
| `spring.r2dbc.password` | `postgres` | Puede sobreescribirse con `POSTGRES_PASSWORD` |
| `server.port` | `8081` | Puerto HTTP |
| `app.external.base-url` | `http://localhost:8081` | Base del simulador interno |
| `app.reservation-ttl` | `15m` | Tiempo de expiracion de una asignacion |
| `app.expiry-interval` | `30s` | Frecuencia del job |
| `app.risk-threshold` | `80` | Limite de rechazo |
| `app.default-risk-score` | `30` | Score usado ante timeout |

## Base de datos: PostgreSQL local (no Docker)

Este proyecto corre contra una instalación nativa de PostgreSQL en la máquina local puesto que en el equipo del banco no contamos con Docker. Solo hace falta que la base `despachodb` exista y que las credenciales en `application.yml` (o la variable `POSTGRES_PASSWORD`) apunten a tu Postgres local. Al arrancar, Spring Boot crea las tablas y siembra los vehículos de ejemplo automáticamente.

```powershell
.\gradlew.bat compileJava
.\gradlew.bat test --no-daemon --console=plain
.\gradlew.bat bootRun --no-daemon --console=plain
```

La aplicación queda escuchando en `http://localhost:8081`.

## API HTTP

### Crear despacho

`POST /api/despachos`

Headers opcionales: `X-Traza-Id` e `Idempotency-Key`.

```json
{
	"clienteId": 1001,
	"ciudad": "BOG",
	"paquetes": [
		{ "vehiculoId": 1, "pesoKg": 80 }
	]
}
```

Respuesta exitosa: `201 Created`.

```json
{
	"id": 10,
	"clienteId": 1001,
	"ciudad": "BOG",
	"estado": "ASIGNADO",
	"tarifa": 10800.00,
	"total": 10800.00,
	"scoreRiesgo": 30,
	"trazaId": "trace-001",
	"paquetes": [
		{ "id": 1, "despachoId": 10, "vehiculoId": 1, "pesoKg": 80 }
	]
}
```

### Consultar y confirmar

- `GET /api/despachos/{id}` devuelve el despacho y sus paquetes.
- `POST /api/despachos/{id}/confirm` solo acepta `ASIGNADO`, cambia a `EN_RUTA` y libera `reservado_kg`.

### Eventos SSE

`GET /api/despachos/{id}/events` devuelve `text/event-stream`, filtra por despacho y termina en `ENTREGADO`, `RECHAZADO` o `EXPIRADO`.

`GET /api/ops/tablero` devuelve el flujo global hot compartido. Usa multicast y `onBackpressureLatest` para conservar el evento mas reciente ante consumidores lentos.

Ejemplo de evento:

```json
{
	"despachoId": 10,
	"estado": "ASIGNADO",
	"mensaje": "Despacho asignado",
	"trazaId": "trace-001",
	"instante": "2026-09-22T12:00:00Z"
}
```

### Vehiculos

- `GET /api/vehiculos` lista vehiculos ordenados por identificador.
- `GET /api/vehiculos/{id}` consulta un vehiculo.
- `POST /api/vehiculos` crea o actualiza por `id`.
- `PUT /api/vehiculos/{id}` actualiza un vehiculo existente.
- `DELETE /api/vehiculos/{id}` elimina un vehiculo sin despachos dependientes.
- `POST /api/vehiculos/bulk` acepta `application/x-ndjson`, procesa lotes de 500 y devuelve `{ "procesados": 501 }`.

Ejemplo de vehiculo:

```json
{ "id": 4, "placa": "LMN321", "ciudad": "BOG", "cupoKg": 400 }
```

### Reportes

- `GET /api/reports/ciudades` devuelve kilos y valor agrupados por ciudad.
- `GET /api/reports/ciudades/stream` devuelve `application/x-ndjson` con el acumulado por ciudad.

### Simulador externo

- `GET /external/simulator` consulta el estado.
- `PUT /external/simulator` configura `fallo`, `latenciaMs` y `riesgo`.
- `DELETE /external/simulator` restablece los valores iniciales.

```json
{
	"fallo": 0,
	"latenciaMs": 250,
	"riesgo": 30
}
```

Endpoints internos consumidos por `WebClient`:

- `GET /external/pricing?ciudad=BOG&peso=80`
- `GET /external/risk?ciudad=BOG`
- `GET /external/window?ciudad=BOG`

## Respuestas de error

Formato comun:

```json
{
	"codigo": 409,
	"mensaje": "Cupo insuficiente o vehiculo inexistente",
	"trazaId": "trace-001",
	"instante": "2026-09-22T12:00:00Z"
}
```

| Situacion | HTTP |
|---|---:|
| Cuerpo invalido | 400 |
| Despacho inexistente | 404 |
| Cupo insuficiente o vehiculo inexistente | 409 |
| Estado invalido | 409 |
| Riesgo superior al limite | 422 |

## Reactividad y backpressure

Esta es la "chuleta" rápida de qué operador se usa dónde y para qué (más abajo, en la tabla de evidencia, está el detalle con archivo y línea exacta):

| Elemento | Ubicacion | Uso |
|---|---|---|
| `Mono` / `Flux` | Controllers y servicios | Contratos asincronos y flujos de datos |
| `concatMap` | `DespachoService` | Reservas ordenadas paquete por paquete |
| `Mono.zip` | `DespachoService` | Consultas externas independientes en paralelo |
| `DatabaseClient` + `RETURNING` | `DespachoService` | Reserva atomica de capacidad |
| `TransactionalOperator` | `DatabaseConfig` / `DespachoService` | Persistencia reactiva transaccional |
| `Sinks.many().multicast()` | `EventBus` | Bus hot de eventos |
| `publish().refCount()` | `TableroController` | Stream compartido del tablero |
| `onBackpressureLatest` + `distinctUntilChanged` | `TableroController` | Conserva el estado mas reciente y descarta notificaciones repetidas |
| `groupBy` + `flatMapIterable` | `ReporteController` | Agrupa filas por ciudad y aplana la lista ordenada resultante |
| `publishOn(Schedulers.parallel())` | `VehiculoController` | Offloadea la validacion de placas en la carga masiva NDJSON |
| `limitRate` | `ReporteController` | Limita la demanda del reporte |
| `scan` | `ReporteController` | Construye el acumulado NDJSON |
| `Flux.interval` / `onBackpressureDrop` | `ExpiracionDespachosJob` | Job periodico sin solapar pasadas |
| Reactor Context | `TraceWebFilter` | Propagacion de `trazaId` |

## Pruebas

Las pruebas actuales son unitarias, corren rápido y no necesitan PostgreSQL levantado. Cubren cosas como:

- Bus multicast y ausencia de replay para suscriptores tardios.
- Retry de tarifa ante errores transitorios y ausencia de retry ante `4xx`.
- Fallback de tarifa y timeout de riesgo con tiempo virtual.
- Configuracion, reset y latencia del simulador.
- Reactor Context y generacion de `X-Traza-Id`.
- Recepcion de eventos del tablero, incluida la deduplicacion con `distinctUntilChanged`.
- Acumulacion del reporte NDJSON.
- Construccion testeable del job de expiracion.
- Contrato basico del controlador de despachos.

Para correr la suite:

```powershell
.\\gradlew.bat test --no-daemon --console=plain
```

El flujo completo (crear despacho, confirmar, reportes, carga masiva) también se probó a mano, con la app corriendo contra el Postgres local real descrito arriba, y funcionó de punta a punta.

## Como quedó al último chequeo

```text
compileJava: correcto
test: 23 pruebas, 23 exitosas, 0 fallos
flujo completo probado a mano contra Postgres local real: crear despacho, confirmar, reportes, carga masiva -> todo OK
busqueda de block(), blockFirst(), blockLast(), Thread.sleep, JdbcTemplate y @Transactional en src/main: sin coincidencias
```

La suite de tests corre sola y no necesita Postgres levantado (usa mocks y `WebTestClient`). Además, en esta sesión se levantó el servicio completo contra un Postgres local real y se probó el recorrido de punta a punta a mano (con `curl`/`Invoke-RestMethod`), así que ya está confirmado que también funciona con base de datos de verdad, no solo en las pruebas unitarias.

## Tabla de evidencia: elemento reactivo -> archivo:linea

| Elemento reactivo | Archivo:linea | Proposito |
|---|---|---|
| `Mono.zip` | `service/DespachoService.java:46` | Ejecuta tarifa, ventana y riesgo en paralelo antes de reservar cupo |
| `concatMap` (reserva secuencial) | `service/DespachoService.java:58` | Reserva paquetes en orden y permite registrar exactamente los reservados para compensar |
| `flatMap` (composicion dependiente) | `service/DespachoService.java:47` | Encadena la decision de riesgo con la creacion o busqueda por idempotencia |
| `onErrorResume` + saga de compensacion | `service/DespachoService.java:67` | Libera cupo reservado si la persistencia o un paquete posterior falla |
| `UPDATE ... RETURNING` atomico | `service/DespachoService.java:25` (constante `RESERVE`) | Reserva de cupo condicionada a `cupo_kg >= peso` sin bloquear hilos |
| `TransactionalOperator` | `service/DespachoService.java:59-65` | Limita la transaccion a insertar despacho, paquetes y cambiar a `ASIGNADO` |
| `retryWhen(Retry.backoff(...))` | `service/ServiciosExternosClient.java:21` | Reintenta la tarifa ante errores 5xx transitorios |
| `onErrorReturn` (fallback) | `service/ServiciosExternosClient.java:22` | Devuelve tarifa de catalogo si el reintento se agota |
| `timeout(Duration)` | `service/ServiciosExternosClient.java:27` | Limita a 800 ms la consulta de riesgo y cae a score por defecto |
| `cache(Duration)` | `service/ServiciosExternosClient.java:31` | Evita repetir la consulta de ventana durante 10 minutos |
| `Sinks.many().multicast()` (hot) | `service/EventBus.java:10` | Bus de eventos compartido entre SSE por despacho y tablero |
| `Flux.merge` | `service/EventBus.java:23` | Combina el flujo de estados activos con el de estados terminales en un unico stream |
| `publish().refCount()` (hot compartido) | `controller/TableroController.java:22` | El tablero conecta una sola vez al bus para todos los suscriptores SSE |
| `onBackpressureLatest` | `controller/TableroController.java:22` | Un consumidor SSE lento del tablero recibe el ultimo estado, no una cola creciente |
| `onBackpressureDrop` | `service/ExpiracionDespachosJob.java:45` | El job descarta ticks del intervalo si el ciclo anterior aun se ejecuta |
| `limitRate(100)` | `controller/ReporteController.java:29` | Limita la demanda al consultar miles de filas del reporte por ciudad |
| `contextWrite` | `config/TraceWebFilter.java:23` | Escribe `trazaId` en el Reactor Context al entrar cada request |
| `Mono.deferContextual` | `service/DespachoService.java:43`, `config/GlobalErrorHandler.java:18` | Lee `trazaId` del Context para logs y para el cuerpo de error, sin recibirlo como parametro |
| `@RestControllerAdvice` + `@ExceptionHandler` | `config/GlobalErrorHandler.java:14,16,21` | Traduce excepciones de dominio a `{codigo, mensaje, trazaId, instante}` |
| SSE con `takeUntil` | `controller/DespachoController.java:49` | Cierra el stream de un despacho al llegar a un estado terminal |
| NDJSON de entrada (`buffer(500)`) | `controller/VehiculoController.java:53` | Carga masiva de vehiculos en lotes con upsert |
| NDJSON de salida (`scan`) | `controller/ReporteController.java:34` | Acumula el reporte por ciudad en un stream `application/x-ndjson` |
| `Disposable` + `@PreDestroy` | `service/ExpiracionDespachosJob.java:28,78` | Cierra ordenadamente la suscripcion de larga vida del job al destruir el bean |
| `doOnCancel` / `doFinally` | `service/ExpiracionDespachosJob.java:73,50` | Observa cancelacion y finalizacion del ciclo de vida del job periodico |
| `StepVerifier.withVirtualTime` | `test/ServiciosExternosClientTest.java` | Prueba retry y timeout sin esperar tiempo real |
| `TestPublisher` | `test/DespachoEventsTestPublisherTest.java` | Controla manualmente la emision de eventos para validar `takeUntil` y `timeout` del SSE |
| `WebTestClient` | `test/DespachoControllerTest.java` | Verifica status, headers y cuerpo del endpoint `POST /api/despachos` |

## Pendientes conocidos

- **Repositorio Git:** el proyecto aun no esta inicializado como repositorio Git en este entorno. Falta `git init`, historial de commits de los integrantes y remoto compartido antes de la entrega (requisito obligatorio, se revisa con `git log --format='%an'`).
- **Pruebas E2E con PostgreSQL real:** flujo completo `POST /api/despachos`, compensacion bajo fallo real, invariante `cupo_kg >= 0` con concurrencia, expiracion real y bulk NDJSON contra la base. La suite actual prueba controladores, servicios y utilidades de forma aislada con mocks y `WebTestClient` sin contexto completo.
- **Sustentacion:** preparar la demo en vivo de 10 minutos con los 3 integrantes y repasar cada linea de la tabla de evidencia para poder explicarla (criterio T6).

## Contenido para compartir

El proyecto puede comprimirse excluyendo artefactos generados y recursos locales:

- Excluir `build/` y `.gradle/`.
- Excluir `.tools/`, `target/` y archivos de log.
- Conservar codigo fuente, pruebas, wrapper de Gradle, configuracion, esquema, Docker Compose y documentacion.

El archivo comprimido debe regenerarse despues de cada cambio relevante para que represente exactamente el estado validado.

## Estado tecnico conocido

- La tarifa externa se consulta, se persiste y utiliza el calculo local como fallback.
- El score externo se persiste; el valor `30` se usa ante timeout o fallo.
- El reporte SQL agrega primero los kilos por despacho para evitar multiplicar el valor monetario por la cantidad de paquetes.
- La idempotencia esta respaldada por una restriccion `UNIQUE`; el replay bajo carreras concurrentes requiere validacion con una base real.
- El bus es multicast sin replay historico.
- PostgreSQL es necesario para ejecutar la aplicacion completa porque los servicios usan `DatabaseClient`.

## Estructura principal

```text
src/main/java/com/example/despachoreactive/
	config/       configuracion, transacciones, errores y trazabilidad
	controller/   endpoints HTTP, SSE, reportes y simulador
	dto/          requests, responses y eventos
	service/      flujo de despacho, integraciones, bus y expiracion
src/main/resources/
	application.yml
	schema.sql
src/test/java/com/example/despachoreactive/
	pruebas unitarias reactivas y de contratos HTTP
```
