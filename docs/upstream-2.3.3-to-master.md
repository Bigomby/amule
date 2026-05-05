# aMule v2.3.3 → v3.0.0 (origin/master) — Upstream Audit

> Resumen de cambios upstream entre la última release estable (`2.3.3`,
> Feb 2021) y la rama `master` actual (que el equipo upstream marca
> como aMule 3.0.0). **Excluye nuestras propias modificaciones** del
> fork ehinny (Prometheus exporter, instrumentación, watchdog).
>
> Nuestra rama de trabajo: `ehinny/master+prometheus`, basada en
> `origin/master`.

## 1. Resumen ejecutivo

Entre `2.3.3` y `origin/master` hay **299 commits** que tocan **812
ficheros**: **+68 984 / −100 064 líneas** (neto −31 080, principalmente
por la limpieza del sistema de build y de subproyectos legacy).

Tema dominante: **paralelización del I/O a disco mediante hilos
dedicados** (descarga, subida, hashing) para eliminar stalls del hilo
principal. Junto con eso, modernización del proyecto (CMake, wxWidgets
3.2+, limpieza C++17).

---

## 2. Nueva arquitectura de hilos para transferencias

Esta es la sección de cabecera y el motivo por el que rebasamos.

### 2.1 `CPartFileWriteThread` — escritura asíncrona de descargas

`src/PartFileWriteThread.{h,cpp}` (porteado de eMule).

**Función**: descargar el coste de las escrituras a disco del hilo
principal. Cuando se reciben bloques de la red, en lugar de llamar
`CFileArea::FlushAt()` síncronamente (que para todos los sockets, las
subidas y la UI), el hilo principal encola un `ToWrite` al hilo de
escritura, que lo procesa en background.

**Protocolo de la cola** (mailbox sticky-flag):

- Main llama `QueueWrite(CPartFile*, PartFileBufferedData*)`
- El worker espera en `wxCondition` con timeout de 500 ms
- Flag pegajoso `m_bWorkPending` para no perder señales (caso clásico:
  `Signal()` llega justo entre el chequeo y el `WaitTimeout()`)
- Al despertar, hace swap a una lista local y procesa todos los items
  fuera del lock

**Diseño de FlushBuffer en dos fases** (en `CPartFile`):

1. **Fase 1** (main): recorre `m_BufferedData_list`, marca
   `PB_READY → PB_PENDING`, y los encola al hilo de escritura.
2. **Fase 2** (main): recoge los items que el hilo de escritura ya
   marcó `PB_WRITTEN`, actualiza `m_aChangedPart[part]=true` y libera
   memoria.
3. **Fallback**: si el hilo de escritura no está corriendo (p.ej.
   durante arranque/shutdown), escritura síncrona en main.

**Estados del buffer** (compatible con eMule):

| Estado | Significado |
|---|---|
| `PB_READY` | listo para encolarse |
| `PB_PENDING` | el hilo de escritura lo está procesando |
| `PB_WRITTEN` | escrito a disco con éxito |
| `PB_ERROR` | fallo de I/O (EIO, disco lleno); fase 2 lo resetea a `PB_READY` para reintentar |

**Sincronización crítica**: `std::mutex CPartFile::m_hpartfileMutex`
nuevo, protege el descriptor de fichero. Tanto el hilo de escritura
(`FlushAt` = Seek + Write) como el hilo de hash (`HashSinglePart` =
Seek + Read) comparten el fd. Sin el lock, las dos llamadas hacen
race en la posición del fichero. Lock por-fichero (no por-chunk),
~1 ms de overhead por escritura.

**Comparación con eMule**: equivalente. eMule usa Windows IOCP +
`WriteFile()` overlapped; aMule sustituye con lecturas/escrituras
síncronas en un hilo dedicado (válido porque el hilo no tiene
contención).

### 2.2 `CPartFileHashThread` — verificación MD4 asíncrona

`src/PartFileHashThread.{h,cpp}` (nuevo, no estaba en eMule).

**Función**: descarga la verificación MD4 por-parte del hilo
principal. Cuando todas las escrituras de una parte completan
(`m_iWrites <= 0`), encola un `HashJob`.

**Por qué no en main**: `HashSinglePart()` lee ~9.28 MB de disco y
calcula MD4, 100–200 ms en discos lentos — por encima de
`CORE_TIMER_PERIOD`. Hacerlo en main durante un drain de pause/resume
con varias parts dirty bloquea `OnCoreTimer` (UI, dispatch ASIO).

**Estructura del job**:

```cpp
struct HashJob {
    CPartFile *pFile;
    uint16     partNumber;
    CMD4Hash   fileHash;   // capturado al encolar — el fichero
                           // puede borrarse antes de que se ejecute
    bool       fromAICHRecoveryDataAvailable;
};
```

**Diseño**: idéntico al `CPartFileWriteThread` (wxThread + wxCondition
+ flag pegajoso). Postea de vuelta un `CPartFileHashResultEvent` al
main thread. La lookup por `fileHash` es segura ante un borrado entre
encolado y dispatch (el evento simplemente se descarta).

**Disparo**: `FlushBuffer` fase 2 encola hash tras recoger los
`PB_WRITTEN` y vaciar el backlog de `m_aChangedPart`.

### 2.3 `CUploadDiskIOThread` — lectura asíncrona para subidas

`src/UploadDiskIOThread.{h,cpp}` (porteado de eMule, adaptado).

**Función**: descarga las lecturas de disco y la construcción de
paquetes ed2k del hilo principal. Antes, el upload-queue llamaba
`CreateNextBlockPackage()` síncronamente por peer, leyendo del disco
y comprimiendo en main.

**Diferencia con eMule**:
- eMule cachea HANDLEs de fichero + `ReadFile(OVERLAPPED)` + IOCP
- aMule usa `CFileArea` open-on-demand (sin cache de handles, más
  ligero)
- Las lecturas son **síncronas en el hilo de disco** (válido porque
  el hilo es dedicado, sin contención)

**Estructuras clave**:

```cpp
struct OpenFile_Struct {
    uint8    ucMD4FileHash[16];
    uint32   nInUse;       // refcount
    uint64   uFileSize;
    bool     bCompress;
};

struct ReadRequest_Struct {
    OpenFile_Struct*        pFileStruct;
    CUpDownClient*          pClient;
    uint64                  uStartOffset, uEndOffset;
    Requested_Block_Struct* pBlock;
    CFileArea               area;
};
```

**Flujo en dos fases**:

1. Main llama `NewBlockRequestsAvailable()` → flag pegajoso despierta
   al worker
2. Worker llama `StartCreateNextBlockPackage(client)` para des-encolar
3. Worker llama `CFileArea::ReadAt()` síncrono → puebla el buffer
4. Worker llama los métodos estáticos `CreateStandardPackets()` o
   `CreatePackedPackets()` para construir paquetes ed2k
5. Los paquetes se encolan en la send queue del cliente

**Tamaño de chunk adaptativo** (10–128 KiB):

```
chunkSize = clamp(uploadDatarate / 8, 10240, 131072)
```

Apunta a ~125 ms de datos por paquete. Por defecto arranca a 10 KiB
(compat eMule); en enlaces rápidos escala para reducir overhead
por-paquete. Tests upstream: 67.3 MB/s vs 15–18 MB/s con 10 KiB fijo.

**Helpers estáticos**: `CreateStandardPackets()` y
`CreatePackedPackets()` se movieron de `CUpDownClient` a métodos
estáticos de `CUploadDiskIOThread` para que el hilo de disco pueda
llamarlos sin tener que mantener estado de cliente.

### 2.4 `CDownloadBandwidthThrottler` — limitador global de descarga

`src/DownloadBandwidthThrottler.{h,cpp}` (nuevo).

**Función**: aplicar `MaxDownload` como **cap estricto bytes/s**
global en todos los sockets simultáneamente.

**Por qué nuevo**: el throttler antiguo (en `DownloadQueue::Process`
+ `CUpDownClient::SetDownloadLimit`) ajustaba la tasa de cada peer en
~5%/tick contra su propia velocidad, sin imponer nunca el `MaxDownload`
como cap literal. El nuevo es demand-aware: peers rápidos pueden
reclamar capacidad no usada por peers lentos en el mismo tick.

**Implementación**: token bucket global con `std::atomic<int64_t>`:

- `RefillBudget(maxKBps, tickMs)` en cada `DownloadQueue::Process()`
- Cada `OnReceive` ASIO llama `Reserve(wantBytes)` antes de leer
- Si `Reserve()` devuelve 0, el socket suspende lecturas hasta el
  próximo refill
- `Refund(bytes)` devuelve capacidad no usada (TCP partial-read, EOF)

**Por qué no necesita hilo dedicado**: descarga es PULL — el kernel
dispara `OnReceive` cuando llegan bytes. Subida es PUSH — los slots
ociosos esperan a que haya datos, lo que sí requiere un hilo que les
despierte. Con descarga basta con un budget atómico compartido.

### 2.5 `m_aChangedPart` — tracking persistente de parts sucias

**Problema**: si un drain de pause/resume llena la cola más rápido de
lo que el hilo de hash drena, una parte podría re-escribirse sin
re-hashearse.

**Solución**: array `bool m_aChangedPart[PARTCOUNT]` en `CPartFile`.

- `FlushBuffer` fase 1 marca `m_aChangedPart[part]=true` tras escribir
- Fase 3 (drain de hash async) limpia bits cuando las parts verifican
- En `~CPartFile`, las parts dirty restantes se hashean síncronamente
  con el lock `m_hpartfileMutex` adquirido

### 2.6 Cambios en `EMSocket` / ASIO

**`956874f9f`** — `wxMutex m_sendLocker` → `std::mutex` (más rápido en
MinGW).

**Bug RC4 stream desync** (parte de `ee5289884`):

- `m_sendBuffer` y `m_blocksWrite` se compartían entre el hilo del
  throttler y el thread pool de ASIO sin sincronización
- `DispatchWrite()` leía `m_sendBuffer` desde el strand de ASIO, pero
  el throttler podía reemplazarlo → mismos bytes enviados dos veces
  → desync del cifrador RC4
- Fix: ambos pasaron a `std::atomic`, captura del puntero al dispatch,
  `compare_exchange_strong` en `HandleSend` para limpiar solo si
  sigue siendo nuestro buffer

**Fix EPOLLET spurious wakeups** (Linux ASIO, parte de `ee5289884`):

- `available()/read_some()` race: `available()` dimensiona el buffer,
  pero datos podían llegar entre eso y `read_some()`. Bajo edge-
  triggered, el kernel no re-arma hasta que el buffer queda vacío y
  vuelve a llenarse. Socket se quedaba parado indefinidamente.
  Fix: bucle `available() + read_some()` hasta drenar.
- `available() == 0` se trataba como EOF, pero los wakeups espurios
  también devuelven 0. El socket se mataba aún con la conexión viva.
  Fix: probe con `recv(MSG_PEEK | MSG_DONTWAIT)` — EOF real devuelve
  0, espurios devuelven −1/EAGAIN.

### 2.7 Trazado de un bloque por la pipeline

Bloque de 1 MB recibido de un peer, parte de 9.28 MB:

1. **ASIO OnReceive** (hilo ASIO, async): kernel entrega los bytes
2. **`CEMSocket::OnReceive`** (hilo ASIO):
   `CDownloadBandwidthThrottler::Reserve()` chequea cuota; lee hasta el
   límite; llama `CUpDownClient::ProcessPayload()`
3. **`CPartFile::WriteToBuffer`** (main): añade a `m_BufferedData_list`,
   incrementa `m_iWrites` (atómico)
4. **`DownloadQueue::Process` tick** (main): llama `FlushBuffer()`
   - Fase 1: `PB_READY → PB_PENDING`, encola al hilo de escritura
   - Fase 2: recoge `PB_WRITTEN`, marca `m_aChangedPart`
5. **`CPartFileWriteThread::Entry`** (hilo de escritura): des-encola,
   bloquea `m_hpartfileMutex`, `FlushAt()`, decrementa `m_iWrites`,
   marca `PB_WRITTEN`
6. **Check de hash** (main, cuando `m_iWrites <= 0` y gaplist
   completa): encola al hilo de hash
7. **`CPartFileHashThread::Entry`** (hilo de hash): des-encola, bloquea
   `m_hpartfileMutex`, `HashSinglePart()`, postea evento
8. **`OnHashResult`** (main, wxEvent): verifica, actualiza estado,
   limpia `m_aChangedPart[part]`

**Puntos de sincronización**:

| Primitiva | Protege |
|---|---|
| `CPartFile::m_hpartfileMutex` | race de Seek entre escritura y hash sobre el mismo fd |
| `m_bytesAvailable` (atomic) | cuota global de descarga |
| `m_iWrites` (atomic) | poll de finalización sin lock |
| `m_aChangedPart[i]` | bits dirty por-parte |

---

## 3. Otros cambios significativos

### 3.1 Build system: autotools → CMake

`CMakeLists.txt` ya existía en `2.3.3` pero junto a autotools. En
master se eliminaron `configure.ac`, `Makefile.am`, `aclocal.m4`,
`autogen.sh`, etc.

- Mínimo CMake 3.10 (antes 3.5)
- Sin `./configure`; ahora `cmake -B build && cmake --build build`
- Eliminados: soporte Qt4, scripts macOS Carbon, proyectos VS
  2010/2013

### 3.2 Modernización C++

- Retirado `throw()` (deprecated en C++17)
- Fix de violaciones rule-of-three (`-Wdeprecated-copy`)
- Migración de `wxConvFile`/`wxConvLocal` a APIs modernas
- `wxPostEvent` → `wxQueueEvent`
- `wxT()` y `wxEmptyString` retirados a nivel árbol

### 3.3 Versiones mínimas de dependencias

| Dependencia | 2.3.3 | 3.0.0 |
|---|---|---|
| wxWidgets | 2.8.12 | **3.2.0** |
| Boost | 1.47 | **1.70** |
| Crypto++ | 5.6 | 5.6 |
| CMake | 3.5 | 3.10 |

### 3.4 Cambios de protocolo

- **Kad**: `ALPHA_QUERY` 3 → 5 (más queries iniciales en search)
- **Kad**: expuesto `CSearch::RequestMoreResults()` (search widening
  por usuario)
- **ed2k**: sin cambios breaking en wire format. Los chunks adaptativos
  son sólo de construcción de paquetes (sender-side), no afectan a
  serialización

### 3.5 Features y subproyectos eliminados

- PlasmaMule (widget KDE4, EOL 2016)
- XAS (plugin XChat 2, sin release desde 2010)
- Frontend Ampache PHP (huérfano)
- Builds Xcode legacy macOS (Carbon-era)
- Proyectos VS 2010 / VS 2013

### 3.6 GUI / UX

- HTTP downloads reescritos sobre `wxWebRequest` (WinHTTP, NSURLSession,
  curl fallback)
- AppImage: nuevo `AppImageIntegration.cpp`, prompt de integración
  desktop
- Workflows de packaging para Linux (.AppImage), macOS (.dmg),
  Windows (.zip)
- macOS: dock-restore en Reopen, icono de tray opcional
- Wayland `app_id` modernizado

### 3.7 Bug fixes notables

- **`27f4a91cd`** — crash de shutdown: `OnExit()` liberaba `uploadqueue`
  antes de parar el `uploadDiskIOThread`. Si el hilo iteraba la lista
  cuando se liberaba → uso de memoria liberada → crash. Fix: parar el
  hilo de disco primero.
- **`ee5289884`** — RC4 desync (ver 2.6) y EPOLLET spurious wakeups.
- **`08c50d686`** — race en `m_iWrites` (decremento en hilo de
  escritura, lectura en main). Fix: atomicidad.
- **`9fc3bd079`** — varias deprecaciones de wxWidgets 3.2.

### 3.8 CI / Release automation

- Workflows GitHub Actions para Linux/Windows/macOS
- Versión derivada del git tag automáticamente
- Fallback a `.git_archival.txt` cuando no hay `.git/` (tarballs de
  distro)

---

## 4. Gotchas en producción al pasar de 2.3.3 a master

1. **Modelo de hilos nuevo** — primera release con workers
   disco/hash en aMule. Vigilar races en deploy (handle contention,
   errores de disco bajo carga). Backlog de hash bajo pause/resume
   visible como spikes de latencia.

2. **`m_hpartfileMutex` es lock por-fichero entero** — todas las
   escrituras y hashes de una parte serializan por un único mutex.
   Multi-fichero ok; dentro de un mismo fichero el paralelismo está
   limitado.

3. **wxWidgets 3.2.0+ obligatorio** — no hay fallback a 3.0/2.8.
   Distros antiguas necesitarán backport o build desde fuente.

4. **Boost 1.70+ obligatorio** — salto desde 1.47.

5. **Sin autotools** — los recipes que dependan de `./configure`
   tienen que reescribirse a CMake.

6. **Cap de descarga estricto** — `MaxDownload` ahora es literal.
   Antes se podía ver overshoot del 5–10%; ahora se clampa. Bajar
   `MaxDownload` un 5–10% si quieres headroom para bursts.

7. **Compatibilidad ED2K wire intacta** — peers viejos y nuevos
   interoperan sin problemas.

8. **HTTP downloads cambian de motor** — verificar SSL y
   compresión/redirects en el server al que se conecte.

9. **Orden de shutdown crítico** — el hilo de disco para subidas
   debe parar antes de destruir la upload queue (fix
   `27f4a91cd`). No re-arrancar tras `EndThread()`.

10. **Default Release** — CMake genera `Release` por defecto (antes
    autotools tenía `--enable-debug` como flag explícito). Sin
    impacto medible en performance.

---

## Tabla resumen de cambios arquitectónicos

| Componente | 2.3.3 | 3.0.0 | Impacto |
|---|---|---|---|
| Escrituras de descarga | main thread (sync) | `CPartFileWriteThread` | sin stalls del main por I/O, ~50–100 ms por bloque |
| Verificación MD4 | main thread | `CPartFileHashThread` | 100–200 ms fuera de main, no congela UI |
| Lecturas de subida | main thread | `CUploadDiskIOThread` | 3–4× throughput tested (67 MB/s vs 15 MB/s) |
| Tamaño de chunk subida | 10 KiB fijo | adaptativo 10–128 KiB | mejor para peers rápidos, compat ed2k |
| Limitador de descarga | nudge per-peer ~5%/tick | bucket atómico global | cap literal, scheduling justo |
| Sync write↔hash sobre fd | inexistente | `m_hpartfileMutex` | evita corrupción de datos |
| Build system | autotools + CMake | CMake only | CI más simple |
| wxWidgets | 2.8.12 | 3.2.0+ | API moderna, Wayland |
| Boost | 1.47 | 1.70+ | ASIO moderno |

---

*Documento generado a partir de un análisis de `git log
2.3.3..origin/master` y lectura de los ficheros nuevos en master.
Excluye los cambios propios del fork ehinny.*
