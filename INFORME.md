# Informe TP Nivelador - Sistemas Distribuidos (Roca) - Joaquín Álvarez 107183

## Protocolo

### Safe Socket (short read/write)
Para solucionar el problema de *short read / short write*, se envolvieron las operaciones de socket en loops (`send_all` / `recv_all`). Estos loops continúan transmitiendo y recibiendo datos de forma iterativa hasta completar la cantidad exacta de bytes requeridos.

### Encabezado del Mensaje (Type + Length)
Para delimitar los tipos de mensaje y su final en el flujo TCP, cada paquete cuenta con un encabezado de 5 bytes:
- **Opcode (1 byte)**: Indica el tipo del mensaje:
  - `0` (**DATA**): Contiene datos de apuestas
  - `1` (**ACK**): Confirmacion enviada por el servidor indicando que el lote fue procesado y almacenado correctamente.
  - `2` (**ERR**): Mensaje de error enviado por el servidor ante fallas de validacion, deserializacion o incompatibilidad en el lote.
  - `3` (**FIN**): Señal explicita de fin de transmisión de datos.
- **Payload Length (4 bytes)**: Entero de 32 bits en formato *big-endian* (`uint32`) que especifica el tamaño en bytes del cuerpo del mensaje.

### Confirmacion por Chunk y Reintentos (ACK / ERR)
Por cada chunk de apuestas enviado por el cliente con el opcode `DATA` (`0`), el servidor valida y almacena los datos. Si la operacion es exitosa, el servidor responde con un mensaje `ACK` (`1`). En caso de falla, el servidor responde con un mensaje `ERR` (`2`). Ante la recepcion de un mensaje `ERR`, el cliente efectua el reenvio automatico del chunk afectado (hasta un maximo de 3 reintentos). El servidor se mantiene a la espera del reenvio sobre la misma conexion.

### Fin de Transmisión
Cuando el emisor concluye el envio de la totalidad de los datos (chunks de apuestas desde el cliente o apuestas ganadoras desde el servidor), transmite un mensaje con el opcode `FIN` (`3`).

## Serializacion

Las apuestas se serializan en un formato binario:
- **ID de Agencia**: 4 bytes (`uint32`)
- **Nombre**: 2 bytes de longitud (`uint16`) + bytes UTF-8 del nombre
- **Apellido**: 2 bytes de longitud (`uint16`) + bytes UTF-8 del apellido
- **Documento**: 4 bytes (`uint32`)
- **Fecha de Nacimiento**: 10 bytes (`YYYY-MM-DD`)
- **Numero**: 4 bytes (`uint32`)

Para los campos de texto, en los que el largo es desconocido, se les agrego un header de 2 bytes con el largo de los bytes a recibir.

## Concurrencia servidor

- **Hilos independientes**: El servidor acepta conexiones entrantes en el hilo principal y delega la atencion de cada cliente a un nuevo hilo (`threading.Thread`).
- **Quorum de Agencias**: Para sincronizar el procesamiento del sorteo, se utiliza una condvar(`threading.Condition`). Cada hilo que finaliza la recepcion de apuestas de una agencia registra su ID y espera (`wait()`) a que el total de agencias procesadas alcance el minimo configurado (`AGENCY_QUORUM_MIN`). Una vez alcanzado el quorum, los hilos son notificados (`notify_all()`) para proceder a consultar los ganadores y enviarselos a los clientes.

## Graceful Shutdown
### Servidor
- **Manejo de Señales**: El servidor captura la señal de apagado (`SIGTERM`).
- **Liberacion de recursos**: Al recibirse la señal, se activa un evento global de apagado (`shutdown_event`), se despiertan los hilos bloqueados en la condicion de quorum y se cierran las conexiones de socket activas para desbloquear lecturas/escrituras en curso.
- **Finalizacion limpia**: El servidor espera la terminacionde los hilos de trabajo activos (`join()`) antes de finalizar el proceso.

### Cliente
- **Manejo de Señales**: Escucha las señales `SIGTERM` mediante un canal de Go (`os/signal.Notify`).
- **Interrupcion de Operaciones**: Al recibir una señal a traves de una goroutine dedicada, se activa un flag `shutdownRequested = true`, se loguea el apagado en progreso y se cierra la conexion (`client.conn.Close()`) para desinterrumpir inmediatamente lecturas o escrituras bloqueadas en el socket.
- **Finalizacion limpia**: En los loops de envio de apuestas y recepcion de resultados, se detecta el estado de apagado y se retorna de forma limpia.