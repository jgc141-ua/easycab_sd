#region LIBRARIES
import socket
import sys
import kafka
import time
import threading
import uuid
import requests
import heapq
import math
import ssl # PARA CIFRADO
import urllib3
urllib3.disable_warnings()

#region CONSTANTS
HEADER = 64
FORMAT = "utf-8"
FIN = "FIN"
KAFKA_IP = 0
KAFKA_PORT = 0
PRODUCER = 0
TAXI_ID = 0

#region GLOBAL VARIABLES
actualPos = {"x": 1, "y": 1} 
beforeTaxiState = ["OK", False] # Estado del taxi y si ese estado proviene del sensor
customerERROR = False
disconnect = False

#region KAFKA PRODUCER
# Crear un productor y enviar un mensaje a través de Kafka con un topic y su mensaje
def sendMessageKafka(topic, msg):
    time.sleep(0.2)
    PRODUCER.send(topic, msg.encode(FORMAT))
    PRODUCER.flush()

#region ALGORITMO A*
MOVIMIENTOS = [(0,1), (0,-1), (1,0), (-1,0), (1,1), (1,-1), (-1,1), (-1,-1)]
TAM_MAPA = 20

def heuristica(pos_actual, pos_destino):
    return math.sqrt((pos_actual[0] - pos_destino[0]) ** 2 + (pos_actual[1] - pos_destino[1]) **2) # Distancia Euclidea

def movimiento_esferico(posicion, movimiento):
    newPosX = (posicion[0] + movimiento[0] - 1) % TAM_MAPA + 1
    newPosY = (posicion[1] + movimiento[1] - 1) % TAM_MAPA + 1
    return (newPosX, newPosY)

def algoritmo_a_estrella(inicio, destino):
    listaFrontera = []
    heapq.heappush(listaFrontera, (0,inicio))

    listaInterior = set()

    coste = {inicio: 0}
    rastreo = {inicio: None}
    while listaFrontera:
        _, actualPos = heapq.heappop(listaFrontera)
        if actualPos == destino:
            camino = []
            
            while actualPos is not None:
                camino.append(actualPos)
                actualPos = rastreo[actualPos]

            return camino[::-1]
        
        listaInterior.add(actualPos)
        for movimiento in MOVIMIENTOS:
            vecino = movimiento_esferico(actualPos, movimiento)
            if vecino in listaInterior:
                continue

            nuevo_coste = coste[actualPos] + 1

            if vecino not in coste or nuevo_coste < coste[vecino]:
                coste[vecino] = nuevo_coste
                prioridad = nuevo_coste + heuristica(vecino, destino)
                heapq.heappush(listaFrontera, (prioridad, vecino))
                rastreo[vecino] = actualPos

    return None

#region TAXI MOVEMENT
def resolveDirection(origen, destino):
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

pendingWay = None 
def mover_taxi(destino, customer=None):
    global actualPos, pendingWay, customerERROR

    end = False
    while not disconnect and not end and not customerERROR:
        if pendingWay:
            camino = pendingWay
            pendingWay = None # Se limpia el camino pendiente
        else:
            camino = algoritmo_a_estrella((actualPos['x'], actualPos['y']), destino)

        if camino is None:
            print(f"ERROR!! NO SE PUDO ENCONTRAR UN CAMINO AL {destino}")
            return

        beforeStep = (actualPos['x'], actualPos['y'])
        pendingWay, end = moveStepTaxi(camino, destino, beforeStep, pendingWay, customer)
        time.sleep(1)

    if customerERROR:
        customerERROR = False

def moveStepTaxi(camino, destino, beforeStep, pendingWay, customer):
    global beforeTaxiState
    for step in camino:
        newPosX, newPosY = step

        if beforeTaxiState[0] == "KO":
            # print(f"[KO] Taxi {TAXI_ID} está en KO, guardando el camino pendiente.")
            pendingWay = camino[camino.index(step):]  # Guardamos el camino restante
            break 

        if not disconnect and not customerERROR:
            direccion = resolveDirection(beforeStep, (newPosX, newPosY))

            actualPos['x'] = newPosX
            actualPos['y'] = newPosY

            # print(f"[MOVIMIENTO] Taxi {TAXI_ID} se ha movido a la posición: {actualPos}")

            if actualPos is not None:
                sendMessageKafka("Taxi2Central", f"TAXI {TAXI_ID} MOVIMIENTO {direccion} POSICIÓN {actualPos['x']} {actualPos['y']}")
            else:
                print(f"ERROR!! TAXI NO TIENE UNA POSICIÓN DEFINIDA PARA ENVIAR")

            if (newPosX, newPosY) == destino:
                if customer != None:
                    sendMessageKafka("Taxi2Central", f"TAXI {TAXI_ID} DESTINO {destino[0]},{destino[1]} CLIENTE {customer}")
                return pendingWay, True

            beforeStep = (newPosX, newPosY)

        time.sleep(2) # Simulación de movimiento

    return pendingWay, False

#region TAXI REGISTER
# Registrar el taxi en la base de datos
REGISTRY_URL = "https://192.168.24.1:5000"
VERIFY_SSL = False

def registerTaxi():
    try:
        data = {"id": int(TAXI_ID)}
        answer = requests.post(f"{REGISTRY_URL}/register", json=data, verify=VERIFY_SSL)

        if answer.status_code == 200:
            print("TAXI REGISTRADO!!")
            return True
        else:
            print(f"ERROR AL REGISTRAR!! {answer.status_code}")
        
    except Exception as e:
        print(f"ERROR EN LA CONEXIÓN: {e}")

    return False

# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets con SSL
def centralConnSSL(centralIP, centralPort):
    if not registerTaxi():
        return None

    try:
        # Crear contexto SSL para el cliente
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_verify_locations("ca.pem")  # Verificar con la CA
        context.load_cert_chain(certfile="taxi.crt", keyfile="taxi.key")  # Certificado y clave del taxi

        # Crear conexión SSL
        with socket.create_connection((centralIP, int(centralPort))) as createdSocket:
            with context.wrap_socket(createdSocket, server_hostname=centralIP) as ssl_sock:
                # Enviar mensaje de autenticación
                authMessage = f"AUTENTICAR TAXI #{TAXI_ID}"
                ssl_sock.send(authMessage.encode(FORMAT))

                # Recibir respuesta de la central
                respuesta = ssl_sock.recv(HEADER).decode(FORMAT)
                print(f"{respuesta}")

                if respuesta.split("\n")[1] == "VERIFICACIÓN SUPERADA.":
                    sendMessageKafka("Status", f"TAXI {TAXI_ID} ACTIVO.")
                    return ssl_sock

    except ConnectionError:
        print(f"Error de conexión con la central en {centralIP}:{centralPort}")

    except Exception as e:
        print(f"Error generado: {str(e)}")

    return None

# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets
def centralConn(centralIP, centralPort):
    if not(registerTaxi()):
        return None

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as createdSocket:
            createdSocket.connect((centralIP, int(centralPort)))
            authMessage = f"AUTENTICAR TAXI #{TAXI_ID}"
            createdSocket.send(authMessage.encode(FORMAT))
            respuesta = createdSocket.recv(HEADER).decode(FORMAT)
            print(f"{respuesta}")

            if respuesta.split("\n")[1] == "VERIFICACIÓN SUPERADA.":
                sendMessageKafka("Status", f"TAXI {TAXI_ID} ACTIVO.")
                return createdSocket

    except ConnectionError:
        print(f"Error de conexión con la central en {centralIP}:{centralPort}")

    except Exception as e:
        print(f"Error generado: {str(e)}")
        
    return None

#region CUSTOMER SERVICES
# Recibe un servicio, una comprobación o un mensaje final
def receiveServices():
    global customerERROR, disconnect

    consumer = kafka.KafkaConsumer("Central2Taxi", group_id=str(uuid.uuid4()), bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    while not disconnect:
        messages = consumer.poll(5000)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                msg = msg.value.decode(FORMAT)
                # print(msg)

                if msg.startswith(f"TAXI {TAXI_ID},CENTRAL "):
                    taxiAction = msg.split(",")[1].split(" ")[1]

                    if taxiAction.startswith("DESTINO") or taxiAction == "BASE":
                        if taxiAction.startswith("DESTINO"):
                            destino = taxiAction.split(" ")[1]
                            customerCentral = "central"

                        else:
                            destino = (1, 1)
                            customerCentral = "base"

                        threadMoverTaxi = threading.Thread(target=mover_taxi, args=(destino, customerCentral))
                        threadMoverTaxi.start()

                    elif taxiAction == "PARAR":
                        eventsControl("KO")

                    elif taxiAction == "REANUDAR":
                        eventsControl("OK")


                elif msg.startswith(f"TAXI {TAXI_ID} DESTINO"):
                    destinoCoord = msg.split(" ")[3].split(",")
                    destino = (int(destinoCoord[0]), int(destinoCoord[1]))
                    # print(destino)
                    customer = msg.split(" ")[5]

                    threadMoverTaxi = threading.Thread(target=mover_taxi, args=(destino, customer))
                    threadMoverTaxi.start()

                elif msg == (f"TAXI {TAXI_ID} ERROR CUSTOMER"):
                    customerERROR = True

                elif msg == f"FIN {TAXI_ID}":
                    disconnect = True
                    consumer.close()

#region TAXI EVENTS
# Función encargada de enviar las incidencias a la central usando kafka
def sendState2Central(state, event=None):
    if event:
        mensaje = f"TAXI {TAXI_ID} {state} INCIDENCIA {event}"
    else:
        mensaje = f"TAXI {TAXI_ID} {state}"

    sendMessageKafka("Taxi2Central", mensaje)
    # print(f"[KAFKA] Estado enviado a la central: {mensaje}")

# Control de Incidencias
def eventsControl(state, event=None, detectedEvent="", sensor=False):
    global beforeTaxiState
    modified = False

    if beforeTaxiState[0] == "OK" and state == "KO":
        #print(f"[CUIDADO] event recibida")
        beforeTaxiState[0] = "KO"
        beforeTaxiState[1] = sensor
        detectedEvent = event
        modified = True
    elif beforeTaxiState[0] == "KO" and beforeTaxiState[1] == True and state == "OK" and sensor == True:
        #print(f"[SOLUCIONADO] event solucionada")
        beforeTaxiState[0] = "OK"
        beforeTaxiState[1] = True
        modified = True
    elif beforeTaxiState[0] == "KO" and beforeTaxiState[1] == False and state == "OK" and sensor == False:
        #print(f"[SOLUCIONADO] event solucionada")
        beforeTaxiState[0] = "OK"
        beforeTaxiState[1] = False
        modified = True

    #beforeTaxiState = state

    if modified:
        sendState2Central(state, event)
    #print(f"[ESTADO] Estado recibido: {state}")

    return detectedEvent

#region TAXI SENSOR
# Función encargada de recibir el estado de los sensores
sensorActivity = False
def sensorState(sensorIP, sensorPort):
    global disconnect, sensorActivity
    socketAddr = (sensorIP, int(sensorPort))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
        servidor.bind(socketAddr)
        servidor.listen()
        print(f"[ESCUCHA] Esperando estado de los sensores en {sensorIP}:{sensorPort}...")

        detectedEvent = ""
        while not disconnect:
            conn, _ = servidor.accept()
            with conn:
                while not disconnect:
                    try:
                        state = conn.recv(HEADER).decode(FORMAT)
                        if "INCIDENCIA" in state:
                            state, event = state.split(", INCIDENCIA: ")
                        else:
                            event = None

                        detectedEvent = eventsControl(state, event, detectedEvent, sensor=True)

                    except Exception as e:
                        print("Sensor caído")
                        print("Taxi sin sensor, peligro de accidente, taxi parado")
                        disconnect = True

# Función encargada de recibir la conexión del sensor con el digital engine
def sensorConn(sensorIP, sensorPort):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.bind((sensorIP, int(sensorPort)))
            servidor.listen()  
            print(f"[ESPERA] Esperando conexión del sensor en {sensorIP}:{sensorPort}...")
            
            conn, addr = servidor.accept() 
            print(f"[CONECTADO] Sensor conectado desde {addr}")
            return conn  
    except Exception as e:
        print(f"[ERROR] No se pudo establecer conexión con el sensor: {str(e)}")
        return None

#region MAP
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

#region STATUS
# Envía cada 2 segundos un mensaje a la central para comprobar su actividad
def taxiStatus():
    while not disconnect:
        sendMessageKafka("Status", f"TAXI {TAXI_ID} ACTIVO.")
        time.sleep(1)

# Recibe el STATUS de la CENTRAL
def centralStatus():
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

#region MAIN
def main(centralIP, centralPort, sensorIP, sensorPort):
    sensorConnVerified = sensorConn(sensorIP, sensorPort)

    if sensorConnVerified:
        verifiedConn = centralConn(centralIP, centralPort)

        if verifiedConn:
            threadSensorState = threading.Thread(target=sensorState, args=(sensorIP, sensorPort))
            threadServices = threading.Thread(target=receiveServices)
            threadMap = threading.Thread(target=showMap)
            threadTaxiStatus = threading.Thread(target=taxiStatus)
            threadCentralStatus = threading.Thread(target=centralStatus)
            threads = [threadSensorState, threadServices, threadMap, threadTaxiStatus, threadCentralStatus]

            for thread in threads:
                thread.start()


        else:
            print(f"Fallo de autenticación, taxi {TAXI_ID} inhabilitado, no se encuentra en la base de datos")
            sys.exit(1)
    
    else:
        print(f"Fallo de conexión con el sensor")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) == 8:
        centralIP = sys.argv[1]
        centralPort = sys.argv[2]
        KAFKA_IP = sys.argv[3]
        KAFKA_PORT = sys.argv[4]
        sensorIP = sys.argv[5]
        sensorPort = sys.argv[6]
        TAXI_ID = sys.argv[7]

        PRODUCER = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

        # Conexión del taxi con la central y kafka
        main(centralIP, centralPort, sensorIP, sensorPort)

    else:
        print(f"ERROR!! Falta por poner <IP EC_CENTRAL> <PUERTO EC_CENTRAL> <IP BOOTSTRAP-SERVER> <PUERTO BOOTSTRAP-SERVER> <IP EC_S> <PUERTO EC_S> <ID TAXI>")
