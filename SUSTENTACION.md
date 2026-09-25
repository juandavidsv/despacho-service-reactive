# Guion de sustentacion (10 min + 5 min de preguntas)

Reparto sugerido para 3 integrantes. Cada bloque indica quien habla, que mostrar en pantalla
(archivo:linea) y que decir. Practicar con el codigo abierto en el IDE y el simulador
(`/external/simulator`) listo para alternar `fallo`, `latenciaMs` y `riesgo` en vivo.

---

## Bloque 1 (Integrante A) - Flujo principal y composicion (0:00 - 2:30)

**Mostrar:** `service/DespachoService.java` metodo `crear` (linea 42) y `Mono.zip` (linea 46).

Guion:
1. "El caso de negocio: una operadora recibe despachos con varios paquetes por ciudad."
2. Mostrar `Mono.zip` combinando tarifa, ventana y riesgo en paralelo. Explicar que las tres
   llamadas son independientes entre si, por eso se paralelizan en vez de encadenarlas.
3. Mostrar `flatMap` (linea 47) que depende del resultado del riesgo: si supera 80, corta el
   flujo con `ZonaRiesgosaException` (422). Explicar por que aqui **si** importa el orden:
   la decision de idempotencia depende de tener ya el riesgo evaluado.
4. Mostrar `concatMap` (linea 58) reservando paquetes uno por uno. Explicar la diferencia con
   `flatMap`: aqui el orden y la exactitud de "que se reservo" importan porque alimenta la
   compensacion (DECISIONES.md punto 1).

**Frase clave:** "Usamos flatMap cuando el orden no afecta el resultado y concatMap cuando
necesitamos saber exactamente que paso, en que orden, para poder compensar."

---

## Bloque 2 (Integrante A) - Saga de compensacion y transaccion (2:30 - 4:00)

**Mostrar:** `RESERVE` (linea 25), `onErrorResume` (linea 67), `TransactionalOperator` (linea 59-65).

Guion:
1. Mostrar el `UPDATE ... WHERE cupo_kg >= :peso RETURNING id`: la atomicidad la da Postgres,
   no un lock en la app. Si la fila no cumple la condicion, no hay `RETURNING` y se lanza
   `CupoInsuficienteException` (409) o `VehiculoNoExisteException` (404) segun corresponda.
2. Mostrar la lista `reservados` que se va llenando con `doOnNext` y como `onErrorResume`
   dispara `liberar(reservados)` antes de propagar el error: esa es la saga de compensacion.
3. Explicar el limite de la transaccion (DECISIONES.md punto 4): la transaccion R2DBC solo
   cubre el INSERT del despacho, el INSERT de paquetes y el cambio a `ASIGNADO`. Las llamadas
   HTTP externas y la reserva de cupo ocurren **antes**, fuera de la transaccion, porque tienen
   su propio timeout/retry y no deben retener una conexion de base de datos.

**Frase clave:** "La transaccion protege la consistencia de los datos que persistimos, no las
llamadas de red. Por eso el cupo se protege con un UPDATE condicional, y si algo falla despues,
compensamos manualmente en vez de depender de un rollback distribuido."

---

## Bloque 3 (Integrante B) - Resiliencia y demo en vivo (4:00 - 6:30)

**Mostrar:** `service/ServiciosExternosClient.java` completo. Tener abierto `/external/simulator`.

Guion:
1. Mostrar `retryWhen(Retry.backoff(3, 200ms)).filter(this::transitorio)` en `tarifa()`. Explicar
   que solo reintenta errores 5xx (transitorios), no 4xx.
2. **Demo en vivo:** `PUT /external/simulator {"fallo": 1}` y disparar `POST /api/despachos`.
   Mostrar en logs o en la respuesta que cae al fallback de tarifa local.
3. Mostrar `timeout(800ms).onErrorReturn(30)` en `riesgo()`. Explicar el score por defecto.
4. **Demo en vivo:** `PUT /external/simulator {"latenciaMs": 2000}` y mostrar que el despacho
   igual responde rapido porque el timeout de riesgo corta a los 800ms.
5. Mostrar `cache(Duration.ofMinutes(10))` en `ventana()`. Explicar que evita golpear el
   servicio simulado en cada request durante 10 minutos.
6. Volver a `PUT /external/simulator {"fallo": 0, "latenciaMs": 0, "riesgo": 90}` y mostrar el
   422 por zona riesgosa.

**Frase clave:** "Cada dependencia externa tiene una politica de resiliencia distinta porque
falla de forma distinta: reintentamos lo transitorio, ponemos limite de tiempo a lo que se
puede colgar, y cacheamos lo que es lento pero estable."

---

## Bloque 4 (Integrante B) - Hot vs cold y backpressure (6:30 - 8:00)

**Mostrar:** `service/EventBus.java`, `controller/TableroController.java`,
`controller/DespachoController.java` (metodo `eventos`).

Guion:
1. Mostrar `Sinks.many().multicast().directBestEffort()`: un unico bus de eventos hot para
   toda la app. Explicar por que hot y no cold (DECISIONES.md punto 3): todos los suscriptores
   deben ver el mismo evento en el mismo momento, sin duplicar la fuente.
2. Mostrar `bus.eventos().filter(...).takeUntil(...)` en el SSE por despacho: se cierra solo al
   llegar a un estado terminal (ENTREGADO/RECHAZADO/EXPIRADO).
3. Mostrar `publish().refCount(1).onBackpressureLatest()` en el tablero global. Explicar la
   eleccion de backpressure (DECISIONES.md punto 2): un consumidor lento del tablero pierde
   eventos intermedios pero nunca el estado mas reciente, y la app no acumula memoria.
4. Contrastar con `limitRate(100)` en el reporte de ciudades: ahi si necesitamos todas las
   filas, solo controlamos la velocidad de la demanda.

**Frase clave:** "El tablero es un dashboard en vivo, no una bitacora: preferimos el ultimo
estado antes que una cola creciente de eventos viejos. El reporte, en cambio, no puede perder
filas, asi que ahi controlamos la demanda con limitRate en vez de descartar datos."

---

## Bloque 5 (Integrante C) - Contexto, errores y streaming NDJSON (8:00 - 9:15)

**Mostrar:** `config/TraceWebFilter.java`, `config/GlobalErrorHandler.java`,
`controller/VehiculoController.java` (bulk), `controller/ReporteController.java` (stream).

Guion:
1. Mostrar `contextWrite` escribiendo `trazaId` en el Reactor Context al entrar cada request.
2. Mostrar `Mono.deferContextual` en `DespachoService.crear` y en `GlobalErrorHandler`: el
   trazaId viaja sin ser parametro de ningun metodo, se lee del Context donde se necesita.
3. Mostrar el cuerpo uniforme de error `{codigo, mensaje, trazaId, instante}` y como cada
   excepcion de dominio (`CupoInsuficienteException`, `VehiculoNoExisteException`,
   `ZonaRiesgosaException`, `DespachoNoExisteException`, `EstadoInvalidoException`) mapea a un
   HTTP status especifico.
4. Mostrar `buffer(500)` en la carga NDJSON de vehiculos y `scan(...)` en el reporte NDJSON de
   salida: dos ejemplos de streaming estructurado, uno de entrada y otro de salida.

**Frase clave:** "El trazaId nunca es un parametro de metodo: vive en el Reactor Context desde
el filtro de entrada hasta el manejador de errores, igual que un ThreadLocal pero seguro para
codigo asincrono."

---

## Bloque 6 (Integrante C) - Ciclo de vida y pruebas (9:15 - 10:00)

**Mostrar:** `service/ExpiracionDespachosJob.java`, la tabla de evidencia del README, y correr
`.\gradlew.bat test --no-daemon --console=plain` en vivo si el tiempo lo permite.

Guion:
1. Mostrar `Flux.interval(30s).onBackpressureDrop()` y el `Disposable suscripcion` con
   `@PreDestroy` para cerrar limpio al apagar la app.
2. Mostrar `doOnCancel` / `doFinally` como observabilidad del ciclo de vida del job.
3. Cerrar mencionando la suite: `StepVerifier`, `StepVerifier.withVirtualTime` (para probar
   retry/timeout sin esperar tiempo real), `TestPublisher` (para simular eventos y validar
   `takeUntil`), y `WebTestClient` (para probar el controller sin levantar servidor real).
4. Cerrar con el numero de pruebas verdes y la ausencia de `block()`/`Thread.sleep`/JDBC en
   `src/main`.

**Frase clave:** "Probamos el tiempo con tiempo virtual, probamos los eventos con un publisher
controlado a mano, y probamos el HTTP con un cliente reactivo de pruebas. En ningun punto de
`src/main` bloqueamos un hilo."

---

## Preguntas frecuentes esperadas (preparar respuesta de 20-30 segundos cada una)

1. **"¿Por que no usaron `@Transactional` de Spring?"** Porque es imperativo y bloqueante sobre
   hilos reactivos; usamos `TransactionalOperator`, que es reactivo y se aplica explicitamente
   sobre el `Mono` que queremos envolver.
2. **"¿Que pasa si el proceso muere justo despues de reservar cupo?"** Queda una reserva sin
   despacho persistido. Mitigacion actual: ninguna automatica todavia; se documenta como riesgo
   en DECISIONES.md punto 4 y se propone una tabla de reservas u outbox como mejora futura.
3. **"¿Por que Sinks y no un Flux.create simple?"** Sinks da control explicito y seguro para
   multiples hilos sobre cuando y como emitir, con estrategias de manejo de fallos de emision
   (`EmitFailureHandler.FAIL_FAST`).
4. **"¿Como garantizan que el idem_key no duplique despachos bajo concurrencia?"** El indice
   `UNIQUE` en `idem_key` en Postgres es la garantia real; el `switchIfEmpty` en la app es una
   optimizacion para no golpear la base en el caso feliz, pero la unicidad final la da la
   restriccion de base de datos.
5. **"¿Por que 800ms para riesgo y no otro valor?"** Es configurable en `application.yml`
   (`app.external.risk-timeout`), elegido para balancear UX y tolerancia a lentitud puntual del
   servicio simulado.

---

## Checklist previo a la demo

- [ ] `docker compose up -d` con Postgres arriba.
- [ ] `.\gradlew.bat bootRun` corriendo sin errores.
- [ ] `/external/simulator` resetado (`DELETE /external/simulator`) antes de empezar.
- [ ] Postman/Insomnia o `curl` con las peticiones de ejemplo ya guardadas.
- [ ] Los 3 integrantes saben en que archivo:linea esta "su" bloque, no solo lo que dicen.
