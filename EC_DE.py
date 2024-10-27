# Digital Engine
# Aplicación que implementará la lógica principal de todo el sistema

# Librerías
import socket # Para establecer la conexión de red entre las aplicaciones del sistema
import sys # Para acceder a los argumentos de la línea de comandos
import kafka
import time
import threading
import uuid

# Para el algoritmo A* --> librerías
import heapq
import math

HEADER = 64
FORMAT = 'utf-8'
FIN = 'FIN'
KAFKA_IP = 0
KAFKA_PORT = 0
PRODUCER = 0
ID_TAXI = 0

# Inicialización de la posición del taxi (indefinida)
posicion_actual = {'x': 1, 'y': 1} 

# Para el algoritmo A* --> dimensiones del mapa
TAM_MAPA = 20

# Para el algoritmo A* --> movimientos
MOVIMIENTOS = [(0,1), (0,-1), (1,0), (-1,0), (1,1), (1,-1), (-1,1), (-1,-1)]
beforeTaxiState = ["OK", False] # Estado del taxi y si ese estado proviene del sensor
sensorActivity = False
customerERROR = False
disconnect = False

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

# Función encargada de mover el taxi y enviar la nueva posición a Kafka
camino_pendiente = None  # Variable global para almacenar el camino pendiente

def mover_taxi(destino, customer=None):
    global posicion_actual, camino_pendiente, customerERROR

    end = False
    while not disconnect and not end and not customerERROR:  # Bucle para permitir que el taxi revise su estado continuamente
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

        camino_pendiente, end = moveStepTaxi(camino, destino, paso_anterior, camino_pendiente, customer)

        # Aquí puedes agregar un pequeño retraso antes de volver a revisar el estado
        time.sleep(1)  # Espera 1 segundo antes de volver a verificar el estado

    if customerERROR:
        customerERROR = False

# Mover un paso el taxi
def moveStepTaxi(camino, destino, paso_anterior, camino_pendiente, customer):
    global beforeTaxiState
    # Mueve el taxi por cada paso en el camino
    for paso in camino:
        nueva_posX, nueva_posY = paso

        # Verificamos si el estado es KO
        if beforeTaxiState[0] == "KO":
            #print(f"[KO] Taxi {ID_TAXI} está en KO, guardando el camino pendiente.")
            camino_pendiente = camino[camino.index(paso):]  # Guardamos el camino restante
            break  # Salimos del bucle for, pero no del bucle while

        if not disconnect and not customerERROR:
            direccion = determinar_direccion(paso_anterior, (nueva_posX, nueva_posY))

            posicion_actual['x'] = nueva_posX
            posicion_actual['y'] = nueva_posY

            #print(f"[MOVIMIENTO] Taxi {id_taxi} se ha movido a la posición: {posicion_actual}")

            # Enviar la nueva posición y dirección a Kafka
            if posicion_actual is not None:
                sendMessageKafka("Taxi2Central", f"TAXI {ID_TAXI} MOVIMIENTO {direccion} POSICIÓN {posicion_actual['x']} {posicion_actual['y']}")
            else:
                print(f"ERROR!! TAXI NO TIENE UNA POSICIÓN DEFINIDA PARA ENVIAR")

            # Verificamos si el taxi ha llegado a su destino
            if (nueva_posX, nueva_posY) == destino:
                if customer != None:
                    sendMessageKafka("Taxi2Central", f"TAXI {ID_TAXI} DESTINO {destino[0]},{destino[1]} CLIENTE {customer}")
                return camino_pendiente, True  # Salimos de la función, no se enviarán más movimientos 

            # Actualizar el paso anterior
            paso_anterior = (nueva_posX, nueva_posY)

        time.sleep(3)  # Simula el tiempo de movimiento

    return camino_pendiente, False

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
def conexion_central(ip_central, port_central):
    try:
        # Creación de un socket IP/TCP
            # AF_INET -> se usará IPv4
            # SOCK_STREAM -> se usará TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
            # Conexión del socket creado con el servidor (central de control) mediante la IP y PUERTO dados
            socket_creado.connect((ip_central, int(port_central)))

            # Autenticación con la central enviando el ID del taxi
            mensaje_de_autenticacion = f'AUTENTICAR TAXI #{ID_TAXI}'

            # Envío del mensaje de autenticación
            socket_creado.send(mensaje_de_autenticacion.encode(FORMAT))

            # Espera de la respuesta de la central
            respuesta = socket_creado.recv(HEADER).decode(FORMAT)

            # Mostramos la respuesta de la central
            print(f"{respuesta}")

            # Comprobamos si la autenticación fue válida o errónea
            if respuesta.split("\n")[1] == "VERIFICACIÓN SUPERADA.":
                sendMessageKafka("Status", f"TAXI {ID_TAXI} ACTIVO.")
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
    PRODUCER.send(topic, msg.encode(FORMAT))
    PRODUCER.flush()

# Recibe un servicio, una comprobación o un mensaje final
def receiveServices():
    global customerERROR, disconnect

    consumer = kafka.KafkaConsumer("Central2Taxi", group_id=str(uuid.uuid4()), bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    while not disconnect:
        messages = consumer.poll(5000)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                msg = msg.value.decode(FORMAT)
                #print(msg)

                if msg.startswith(f"TAXI {ID_TAXI},CENTRAL "):
                    taxiAction = msg.split(",")[1].split(" ")[1]

                    if taxiAction.startswith("DESTINO"):
                        destino = taxiAction.split(" ")[1]

                        threadMoverTaxi = threading.Thread(target=mover_taxi, args=(destino, "central"))
                        threadMoverTaxi.start()

                    elif taxiAction == "BASE":
                        destino = (1, 1)

                        threadMoverTaxi = threading.Thread(target=mover_taxi, args=(destino, "base"))
                        threadMoverTaxi.start()

                    elif taxiAction == "PARAR":
                        incidenciasControl("KO")

                    elif taxiAction == "REANUDAR":
                        incidenciasControl("OK")


                elif msg.startswith(f"TAXI {ID_TAXI} DESTINO"):
                    destinoCoord = msg.split(" ")[3].split(",")
                    destino = (int(destinoCoord[0]), int(destinoCoord[1]))
                    #print(destino)
                    customer = msg.split(" ")[5]

                    threadMoverTaxi = threading.Thread(target=mover_taxi, args=(destino, customer))
                    threadMoverTaxi.start()

                elif msg == (f"TAXI {ID_TAXI} ERROR CUSTOMER"):
                    customerERROR = True

                elif msg == f"FIN {ID_TAXI}":
                    disconnect = True
                    consumer.close()

# Función encargada de enviar la incidencia a la central usando kafka
def enviar_estado_a_central(estado, incidencia=None):
    if incidencia:
        mensaje = f"TAXI {ID_TAXI} {estado} INCIDENCIA {incidencia}"
    else:
        mensaje = f"TAXI {ID_TAXI} {estado}"

    sendMessageKafka("Taxi2Central", mensaje)
    #print(mensaje)

    #print(f"[KAFKA] Estado enviado a la central: {mensaje}")

def incidenciasControl(estado, incidencia=None, incidencia_detectada="", sensor=False):
    global beforeTaxiState
    modified = False

    if beforeTaxiState[0] == "OK" and estado == "KO":
        #print(f"[CUIDADO] Incidencia recibida")
        beforeTaxiState[0] = "KO"
        beforeTaxiState[1] = sensor
        incidencia_detectada = incidencia
        modified = True
    elif beforeTaxiState[0] == "KO" and beforeTaxiState[1] == True and estado == "OK" and sensor == True:
        #print(f"[SOLUCIONADO] Incidencia solucionada")
        beforeTaxiState[0] = "OK"
        beforeTaxiState[1] = True
        modified = True
    elif beforeTaxiState[0] == "KO" and beforeTaxiState[1] == False and estado == "OK" and sensor == False:
        #print(f"[SOLUCIONADO] Incidencia solucionada")
        beforeTaxiState[0] = "OK"
        beforeTaxiState[1] = False
        modified = True

    #beforeTaxiState = estado

    if modified:
        enviar_estado_a_central(estado, incidencia)
    #print(f"[ESTADO] Estado recibido: {estado}")

    return incidencia_detectada

# Función encargada de recibir el estado de los sensores
def recibir_estado_sensor(ip_sensores, port_sensores):
    global disconnect, sensorActivity
    addr_DE = (ip_sensores, int(port_sensores))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
        servidor.bind(addr_DE)
        servidor.listen()
        print(f"[ESCUCHA] Esperando estado de sensores en {ip_sensores}:{port_sensores}...")

        incidencia_detectada = ""

        while not disconnect:
            conn, _ = servidor.accept()
            with conn:
                while not disconnect:
                    try:
                        estado = conn.recv(HEADER).decode(FORMAT)
                        
                        if "INCIDENCIA" in estado:
                            estado, incidencia = estado.split(", INCIDENCIA: ")
                        else:
                            incidencia = None

                        incidencia_detectada = incidenciasControl(estado, incidencia, incidencia_detectada, sensor=True)

                    except Exception as e:
                        print("Sensor caído")
                        print("Taxi sin sensor, peligro de accidente, taxi parado")
                        disconnect = True

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

# Mostrar mapa
def showMap():
    global disconnect

    consumer = kafka.KafkaConsumer("Mapa", group_id=str(uuid.uuid4()), bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

    while not disconnect:
        messages = consumer.poll(1000)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                msg = msg.value.decode(FORMAT)
                print(msg, end="")

# Envía cada 2 segundos un mensaje a la central para comprobar su actividad
def sendCentralActive():
    while not disconnect:
        sendMessageKafka("Taxi2Central", f"ACTIVE?")
        sendMessageKafka("Status", f"TAXI {ID_TAXI} ACTIVO.")
        time.sleep(2)

# Recibe el STATUS de la CENTRAL
def statusControl():
    global disconnect
    consumer = kafka.KafkaConsumer("Central2Taxi", group_id=str(uuid.uuid4()), bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

    startTime = time.time()
    while not disconnect:
        endTime = time.time()
        if endTime - startTime > 10:
            disconnect = True
            break
        
        messages = consumer.poll(1000)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                msg = msg.value.decode(FORMAT)

                if msg == "CENTRAL ACTIVE":
                    startTime = time.time()
        
    consumer.close()

def main(ip_central, port_central, ip_sensores, port_sensores):
    # Conexión con el sensor
    conexion_recibida_sensor = esperar_conexion_sensor(ip_sensores, port_sensores)

    if conexion_recibida_sensor:

        # Conexión con la central
        conexion_verificada = conexion_central(ip_central, port_central)

        if conexion_verificada:
            threadRecibeEstados = threading.Thread(target=recibir_estado_sensor, args=(ip_sensores, port_sensores))
            threadRecibeEstados.start()

            threadServices = threading.Thread(target=receiveServices)
            threadServices.start()

            threadMapa = threading.Thread(target=showMap)
            threadMapa.start()

            threadActiveCentralMSG = threading.Thread(target=sendCentralActive)
            threadActiveCentralMSG.start()

            threadStatusControlCentral = threading.Thread(target=statusControl)
            threadStatusControlCentral.start()


        else:
            print(f"Fallo de autenticación, taxi {ID_TAXI} inhabilitado, no se encuentra en la base de datos")
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
        ID_TAXI = sys.argv[7]

        PRODUCER = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

        # Conexión del taxi con la central y kafka
        main(ip_central, port_central, ip_sensores, port_sensores)

    else:
        print(f"ERROR!! Falta por poner <IP EC_CENTRAL> <PUERTO EC_CENTRAL> <IP BOOTSTRAP-SERVER> <PUERTO BOOTSTRAP-SERVER> <IP EC_S> <PUERTO EC_S> <ID TAXI>")
