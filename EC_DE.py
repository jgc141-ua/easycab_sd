# Digital Engine
# Aplicación que implementará la lógica principal de todo el sistema

# Librerías
import socket # Para establecer la conexión de red entre las aplicaciones del sistema
import sys # Para acceder a los argumentos de la línea de comandos
import kafka
import json
import time
import threading

# Para el algoritmo A* --> librerías
import heapq
import math

HEADER = 64
FORMAT = 'utf-8'
FIN = 'FIN'
KAFKA_IP = 0
KAFKA_PORT = 0

# Inicialización de la posición del taxi (indefinida)
posicion_actual = None

# Para el algoritmo A* --> dimensiones del mapa
TAM_MAPA = 20

# Para el algoritmo A* --> movimientos
MOVIMIENTOS = [(0,1), (0,-1), (1,0), (-1,0), (1,1), (1,-1), (-1,1), (-1,-1)]

# Para el algoritmo A* --> heurística --> distancia euclidea
def heuristica(pos_actual, pos_destino):
    return math.sqrt((pos_actual[0] - pos_destino[0]) ** 2 + (pos_actual[1] - pos_destino[1]) **2)

# Para el algoritmo A* --> movimiento esférico
def movimiento_esferico(posicion, movimiento):
    nueva_posX = (posicion[0] + movimiento[0]) % TAM_MAPA
    nueva_posY = (posicion[1] + movimiento[1]) % TAM_MAPA
    return (nueva_posX, nueva_posY)

# Algoritmo A*
def algoritmo_a_estrella(inicio, destino):
    listaFrontera = []
    heapq.heappush(listaFrontera, (0,inicio))

    listaInterior = set()

    coste = {inicio: 0}
    rastreo = {inicio: None}

    while listaFrontera:
        _, posicion_actual = heapq.heappop(listaFrontera)

        if posicion_actual == destino:
            camino = []
            
            while posicion_actual is not None:
                camino.append(posicion_actual)
                posicion_actual = rastreo[posicion_actual]

            return camino[::-1]
        
        listaInterior.add(posicion_actual)

        for movimiento in MOVIMIENTOS:
            vecino = movimiento_esferico(posicion_actual, movimiento)

            if vecino in listaInterior:
                continue

            nuevo_coste = coste[posicion_actual] + 1

            if vecino not in coste or nuevo_coste < coste[vecino]:
                coste[vecino] = nuevo_coste

                prioridad = nuevo_coste + heuristica(vecino, destino)

                heapq.heappush(listaFrontera, (prioridad, vecino))

                rastreo[vecino] = posicion_actual

    return None

# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets
def conexion_central(ip_central, port_central, id_taxi):
    try:
        # Creación de un socket IP/TCP
            # AF_INET -> se usará IPv4
            # SOCK_STREAM -> se usará TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
            # Conexión del socket creado con el servidor (central de control) mediante la IP y PUERTO dados
            socket_creado.connect((ip_central, int(port_central)))

            # Autenticación con la central enviando el ID del taxi
            mensaje_de_autenticacion = f'AUTENTICAR TAXI #{id_taxi}'

            # Envío del mensaje de autenticación
            socket_creado.send(mensaje_de_autenticacion.encode(FORMAT))

            # Espera de la respuesta de la central
            respuesta = socket_creado.recv(HEADER).decode(FORMAT)

            # Mostramos la respuesta de la central
            print(f"{respuesta}")

            # Comprobamos si la autenticación fue válida o errónea
            if respuesta.split("\n")[1] == "VERIFICACIÓN SUPERADA.":
                sendMessageKafka("Status", f"TAXI {id_taxi} ACTIVO.")
                return socket_creado # Devolvemos el socket para usarlo después
            else:
                return None
        
    except ConnectionError:
        print(f"Error de conexión con la central en {ip_central}:{port_central}")
    except Exception as e:
        print(f"Error generado: {str(e)}")
        return None
    
# Función encargada de enviar eventos del taxi a través de kafka
def enviar_eventos_kafka(productor, id_taxi, mensaje_kafka):
    global posicion_actual

    if posicion_actual is not None:
        evento = {
            'tipo' : 'movimiento',
            'id_taxi' : id_taxi,
            'posicion' : f'x:{posicion_actual["x"]}, y:{posicion_actual["y"]}'
        }

        # Mensaje en kafka
        productor.send(mensaje_kafka, value=evento)
        productor.flush()

        print(f"[EVENTO] Taxi {id_taxi} publicó su posición en kafka: {evento['posicion']}")
    else:
        print(f"[ERROR] Taxi no tiene una posición definida para enviar")

# Función encargada de escuchar instrucciones desde kafka
def recibir_mensajes_kafka(consumidor, id_taxi):
    print(f"[ESPERANDO] Taxi {id_taxi} experando mensajes de la central...")

    for mensaje in consumidor:
        datos = mensaje.value
        print(f"Mensaje recibido de la central: {datos}")

        if datos['tipo'] == 'instruccion':
            print(f"Instrucción recibida: {datos['instruccion']}")
            if datos['instruccion'] == 'parar':
                print(f"Taxi {id_taxi} se detiene")
            elif datos['instruccion'] == 'reanudar':
                print(f"Taxi {id_taxi} reanuda el proyecto")
            elif datos['instruccion'] == 'mover':
                posicion_nueva = datos['posicion_nueva']
                posicion_actual = {'x': posicion_nueva['x'], 'y' : posicion_nueva['y']}
                print(f"Taxi {id_taxi} movido a la nueva posición: {posicion_actual}")
            elif datos['instruccion'] == FIN:
                print(f"La central ha finalizado la conexión con el Taxi {id_taxi}")
                break

# Crear un productor y enviar un mensaje a través de Kafka con un topic y su mensaje
def sendMessageKafka(topic, msg):
    time.sleep(0.2)
    producer = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    producer.send(topic, msg.encode(FORMAT))
    producer.flush()
    producer.close()

# Recibe un servicio, una comprobación o un mensaje final
def receiveServices(id_taxi):
    consumer = kafka.KafkaConsumer("Central2Taxi", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    for msg in consumer:
        message = msg.value.decode(FORMAT)
        print(message)

        if message == "TAXI STATUS":
            sendMessageKafka("Status", f"TAXI {id_taxi} ACTIVO.")
        elif message == f"FIN {id_taxi}":
            consumer.close()

        elif message == "DEATH CENTRAL":
            consumer.close()
            print("SE HA PERDIDO LA CONEXIÓN CON LA CENTRAL.")
            return True

# Función encargada de enviar la incidencia a la central
def enviar_incidencia_a_central(ip_central, port_central, incidencia):
    addr_central = (ip_central, int(port_central))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_central:
        socket_central.connect(addr_central)

        socket_central.send(incidencia.encode(FORMAT))

        print(f"[ENVÍO] Incidencia enviada a la central: {incidencia}")

# Función encargada de recibir el estado de los sensores
def recibir_estado_sensor(ip_sensores, port_sensores):
    addr_DE = (ip_sensores, int(port_sensores))
    estado_anterior = "OK"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
        servidor.bind(addr_DE)
        servidor.listen()
        print(f"[ESCUCHA] Esperando estado de sensores en {ip_sensores}:{port_sensores}...")

        incidencia_detectada = ""

        while True:
            conn, _ = servidor.accept()
            with conn:
                while True:
                    try:
                        estado = conn.recv(HEADER).decode(FORMAT)

                        if "INCIDENCIA" in estado:
                            estado, incidencia = estado.split(", INCIDENCIA: ")
                        else:
                            incidencia = None

                        if estado_anterior == "OK" and estado == "KO":
                            print(f"[CUIDADO] Incidencia recibida: {incidencia}")
                            estado_anterior = "KO"
                            incidencia_detectada = incidencia
                            # enviar_incidencia_a_central(ip_central, port_central, incidencia)
                        elif estado_anterior == "KO" and estado == "OK":
                            print(f"[SOLUCIONADO] Incidencia ({incidencia_detectada}) solucionada")
                            estado_anterior = "OK"

                        print(f"[ESTADO] Estado recibido: {estado}")

                    except Exception as e:
                        print("Sensor caído") 
                        print("Taxi sin sensor, peligro de accidente, taxi parado")
                        sys.exit(1)
 
def main(ip_central, port_central, ip_sensores, port_sensores, id_taxi):
    # Conexión con la central
    conexion_verificada = conexion_central(ip_central, port_central, id_taxi)

    if conexion_verificada:
        threadServices = threading.Thread(target=receiveServices, args=(id_taxi))
        threadServices.start()

# Main
if __name__ == "__main__":
    # Se esperan 8 argumentos
    if len(sys.argv) == 8:
        ip_central = sys.argv[1]
        port_central = sys.argv[2]
        KAFKA_IP = sys.argv[3]
        KAFKA_PORT = sys.argv[4]
        ip_sensores = sys.argv[5]
        port_sensores = sys.argv[6]
        id_taxi = sys.argv[7]

        # Conexión del taxi con la central y kafka
        main(ip_central, port_central, ip_sensores, port_sensores, id_taxi)

        recibir_estado_sensor(ip_sensores, port_sensores)

    else:
        print(f"ERROR!! Falta por poner <IP EC_CENTRAL> <PUERTO EC_CENTRAL> <IP BOOTSTRAP-SERVER> <PUERTO BOOTSTRAP-SERVER> <IP EC_S> <PUERTO EC_S> <ID TAXI>")
