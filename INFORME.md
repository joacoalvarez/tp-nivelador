# Informe TP Nivelador - Sistemas Distribuidos (Roca) - Joaquín Álvarez 107183

## Protocolo

### Safe Socket (short read/write)
Para solucionar el problema de *short read / short write*, se envolvieron las operaciones de socket en loops (`send_all` / `recv_all`). Estos loops continuan transmitiendo y recibiendo datos de forma iterativa hasta completar la cantidad exacta de bytes requeridos.

### Payload Length Header
Para delimitar los mensajes en el flujo de bytes TCP, cada mensaje se le agrega un encabezado de 4 bytes (`uint32` en big-endian) que especifica la longitud del payload.

### Fin Transmision
Cuando el emisor termina la transmision, envia un mensaje (**FIN**) cuyo identificador es el payload de longitud 0.

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