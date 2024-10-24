# Digital Engine
# Aplicación que implementará la lógica principal de todo el sistema

# Librerías
import socket # Para establecer la conexión de red entre las aplicaciones del sistema
import sys # Para acceder a los argumentos de la línea de comandos
import kafka
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
posicion_actual = {'x': 1, 'y': 1} 

# Para el algoritmo A* --> dimensiones del mapa
TAM_MAPA = 20

# Para el algoritmo A* --> movimientos
MOVIMIENTOS = [(0,1), (0,-1), (1,0), (-1,0), (1,1), (1,-1), (-1,1), (-1,-1)]

# Para el algoritmo A* --> heurística --> distancia euclidea
def heuristica(pos_actual, pos_destino):
    return math.sqrt((pos_actual[0] - pos_destino[0]) ** 2 + (pos_actual[1] - pos_destino[1]) **2)

# Para el algoritmo A* --> movimiento esférico
def movimiento_esferico(posicion, movimiento):
    nueva_posX = (posicion[0] + movimiento[0] - 1) % TAM_MAPA + 1
    nueva_posY = (posicion[1] + movimiento[1] - 1) % TAM_MAPA + 1
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

# Función encargada de enviar eventos del taxi a través de Kafka 
def enviar_movimientos_Taxi(productor, id_taxi, topic, direccion):
    global posicion_actual

    if posicion_actual is not None:
        mensaje = f"TAXI {id_taxi} MOVIMIENTO: {direccion}, POSICIÓN: x={posicion_actual['x']}, y={posicion_actual['y']}"

        # Enviar el mensaje a Kafka
        productor.send(topic, value=mensaje.encode(FORMAT))
        productor.flush()  

        print(f"[EVENTO, KAFKA] {mensaje}")
    else:
        print(f"[ERROR] Taxi no tiene una posición definida para enviar")

# Función encargada de mover el taxi y enviar la nueva posición a Kafka
camino_pendiente = None  # Variable global para almacenar el camino pendiente
estado_actual = "OK"   # Inicializa el estado como OK globalmente

def mover_taxi(id_taxi, productor, destino):
    global posicion_actual, camino_pendiente, estado_actual

    while True:  # Bucle para permitir que el taxi revise su estado continuamente
        # Si hay un camino pendiente desde una pausa anterior (estado KO), lo reutilizamos
        if camino_pendiente:
            camino = camino_pendiente
            camino_pendiente = None  # Limpiamos el camino pendiente ya que vamos a continuar
        else:
            # Si no hay camino pendiente, calculamos uno nuevo
            camino = algoritmo_a_estrella((posicion_actual['x'], posicion_actual['y']), destino)

        if camino is None:
            print(f"[ERROR] No se pudo encontrar un camino al destino {destino}")
            return

        paso_anterior = (posicion_actual['x'], posicion_actual['y'])

        # Mueve el taxi por cada paso en el camino
        for paso in camino:
            nueva_posX, nueva_posY = paso

            # Verificamos si el estado es KO
            if estado_actual == "KO":
                print(f"[KO] Taxi {id_taxi} está en KO, guardando el camino pendiente.")
                camino_pendiente = camino[camino.index(paso):]  # Guardamos el camino restante
                break  # Salimos del bucle for, pero no del bucle while

            direccion = determinar_direccion(paso_anterior, (nueva_posX, nueva_posY))

            posicion_actual['x'] = nueva_posX
            posicion_actual['y'] = nueva_posY

            print(f"[MOVIMIENTO] Taxi {id_taxi} se ha movido a la posición: {posicion_actual}")

            # Enviar la nueva posición y dirección a Kafka
            enviar_movimientos_Taxi(productor, id_taxi, 'TaxiMovimiento', direccion)

            # Verificamos si el taxi ha llegado a su destino
            if (nueva_posX, nueva_posY) == destino:
                print(f"[DESTINO] Taxi {id_taxi} ha llegado a su destino: {destino}")
                return  # Salimos de la función, no se enviarán más movimientos

            # Actualizar el paso anterior
            paso_anterior = (nueva_posX, nueva_posY)

            time.sleep(5)  # Simula el tiempo de movimiento

        # Aquí puedes agregar un pequeño retraso antes de volver a revisar el estado
        time.sleep(1)  # Espera 1 segundo antes de volver a verificar el estado

# Determina la dirección del movimiento basado en las coordenadas de origen y destino
def determinar_direccion(origen, destino):
    dx = destino[0] - origen[0]
    dy = destino[1] - origen[1]

    if dx == 0 and dy > 0:
        return "Norte"
    elif dx == 0 and dy < 0:
        return "Sur"
    elif dx > 0 and dy == 0:
        return "Este"
    elif dx < 0 and dy == 0:
        return "Oeste"
    elif dx > 0 and dy > 0:
        return "Noreste"
    elif dx > 0 and dy < 0:
        return "Sureste"
    elif dx < 0 and dy > 0:
        return "Noroeste"
    else:
        return "Suroeste"

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

        if message == "TAXI STATUS":
            sendMessageKafka("Status", f"TAXI {id_taxi} ACTIVO.")
        elif message == f"FIN {id_taxi}":
            consumer.close()

        elif message == "DEATH CENTRAL":
            consumer.close()
            print("SE HA PERDIDO LA CONEXIÓN CON LA CENTRAL.")
            return True

# Función encargada de enviar la incidencia a la central usando kafka
def enviar_estado_a_central(id_taxi, estado, incidencia=None):
    producer = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    
    if incidencia:
        mensaje = f"TAXI {id_taxi}, {estado}, INCIDENCIA: {incidencia}"
    else:
        mensaje = f"TAXI {id_taxi}, {estado}"

    producer.send('InfoEstadoTaxi', mensaje.encode(FORMAT))
    producer.flush()
    producer.close()
    
    print(f"[KAFKA] Estado enviado a la central: {mensaje}")

# Función encargada de recibir el estado de los sensores
def recibir_estado_sensor(id_taxi, ip_sensores, port_sensores):
    global estado_actual
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

                        estado_actual = estado

                        if estado_anterior == "OK" and estado == "KO":
                            print(f"[CUIDADO] Incidencia recibida: {incidencia}")
                            estado_anterior = "KO"
                            incidencia_detectada = incidencia
                            enviar_estado_a_central(id_taxi, estado, incidencia)
                        elif estado_anterior == "KO" and estado == "OK":
                            print(f"[SOLUCIONADO] Incidencia ({incidencia_detectada}) solucionada")
                            estado_anterior = "OK"
                            enviar_estado_a_central(id_taxi, estado, incidencia)
                        else:
                            enviar_estado_a_central(id_taxi, estado, incidencia)

                        print(f"[ESTADO] Estado recibido: {estado}")

                    except Exception as e:
                        print("Sensor caído") 
                        print("Taxi sin sensor, peligro de accidente, taxi parado")
                        sys.exit(1)

# Función encargada de recibir la conexión del sensor con el digital engine
def esperar_conexion_sensor(ip_sensores, port_sensores):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.bind((ip_sensores, int(port_sensores)))
            servidor.listen()  
            print(f"[ESPERA] Esperando conexión del sensor en {ip_sensores}:{port_sensores}...")
            
            conn, addr = servidor.accept() 
            print(f"[CONECTADO] Sensor conectado desde {addr}")
            return conn  
    except Exception as e:
        print(f"[ERROR] No se pudo establecer conexión con el sensor: {str(e)}")
        return None

def main(ip_central, port_central, ip_sensores, port_sensores, id_taxi):
    # Conexión con el sensor
    conexion_recibida_sensor = esperar_conexion_sensor(ip_sensores, port_sensores)

    if conexion_recibida_sensor:

        # Conexión con la central
        conexion_verificada = conexion_central(ip_central, port_central, id_taxi)

        if conexion_verificada:
            threadRecibeEstados = threading.Thread(target=recibir_estado_sensor, args=(id_taxi, ip_sensores, port_sensores))
            threadServices = threading.Thread(target=receiveServices, args=(id_taxi))

            destino = (15, 15)  
            threadMoverTaxi = threading.Thread(target=mover_taxi, args=(id_taxi, kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}"), destino))

            threadRecibeEstados.start()
            threadServices.start()
            threadMoverTaxi.start()

            threadRecibeEstados.join()
            threadServices.join()
            threadMoverTaxi.join()

        else:
            print(f"Fallo de autenticación, taxi {id_taxi} inhabilitado, no se encuentra en la base de datos")
            sys.exit(1)
    
    else:
        print(f"Fallo de conexión con el sensor")
        sys.exit(1)

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

    else:
        print(f"ERROR!! Falta por poner <IP EC_CENTRAL> <PUERTO EC_CENTRAL> <IP BOOTSTRAP-SERVER> <PUERTO BOOTSTRAP-SERVER> <IP EC_S> <PUERTO EC_S> <ID TAXI>")
