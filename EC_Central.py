import sys
import socket
import threading
import kafka
import time
import colorama
from colorama import *
import json

import kafka.errors

# PARA MOSTRAR MAPA
#######################################
import colorama
from colorama import Fore, Back, Style
import sys
import time
#######################################

HEADER = 64
FORMAT = 'utf-8'
END_CONNECTION1 = "FIN"
END_CONNECTION2 = "ERROR"
KAFKA_IP = 0
KAFKA_PORT = 0
PRODUCER = 0

CHAR_MAP = "."
SPACE = " "
LINE = f"{'-' * 64}\n"

# PARA MOSTRAR MAPA
#######################################
mapa = [[["."] for _ in range(20)] for _ in range(20)]
taxis = []
customers = []
locations = []

colorama.init(autoreset=True)

def mostrar_mapa():
    print("\n" * 6)

    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<15} *** EASY CAB Release 1 ***\n")
    sys.stdout.write(LINE)
    
    # Mostrar encabezado de taxis y clientes
    sys.stdout.write(f"{' ':<10} {'Taxis':<19} {'|':<8} {'Clientes'}\n")
    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<3} {'Id.':<5} {'Destino':<10} {'Estado':<9} {'|':<2} {'Id.':<5} {'Destino':<10} {'Estado':<10}\n")
    sys.stdout.write(LINE)

    # Mostrar los taxis y los clientes en filas paralelas
    maxSize = max(len(taxis), len(customers))
    for i in range(maxSize):
        taxi_line = f"{SPACE * 3}"
        cliente_line = f"{SPACE * 3}"
        
        if i < len(taxis):
            taxi = getTaxi(taxis[i])
            if taxi:
                idTaxi = taxi[0]
                destTaxi = taxi[1]
                activeTaxi = taxi[2]
                serviceTaxi = taxi[3].split("\n")[0]
                
                stateTaxi = ""
                if serviceTaxi.startswith("Servicio"):
                    stateTaxi = f"{activeTaxi}. {serviceTaxi}"
                else:
                    stateTaxi = f"{SPACE * 3} {activeTaxi}. {serviceTaxi}"

                taxi_line = f"{SPACE*4} {idTaxi} {SPACE*5} {destTaxi} {stateTaxi}  |"
            else:
                taxi_line = SPACE * 30
        
        if i < len(customers):
            customer = customers[i]
            idCustomer = customer[0]
            ubiCustomer = customer[1]
            destCustomer = customer[2]
            stateCustomer = customer[3]

            cliente_line = f"{idCustomer:<5} {destCustomer:<10} {stateCustomer}"
        
        sys.stdout.write(f"{taxi_line} {cliente_line}\n")

        if i == maxSize - 1:
            sys.stdout.write(LINE)


    sys.stdout.write(LINE)

    sys.stdout.write("   " + " ".join([f"{i:2}" for i in range(1, 21)]) + "\n")
    
    sys.stdout.write(LINE)

    # Mostrar el mapa utilizando streams
    for row in range(len(mapa)):
        # Mostrar el número de fila (ajustado de 1-20)
        sys.stdout.write(f"{row + 1:2} ")
        for col in range(len(mapa[row])):
            mapValue = mapa[row][col][0]

            if isinstance(mapValue, int):  # Taxis
                sys.stdout.write(Fore.GREEN + f"{mapValue:2} " + Style.RESET_ALL)

            elif isinstance(mapValue, str) and len(mapValue) == 1 and mapValue.isalpha():
                if mapValue.isupper():  # Localizaciones
                    sys.stdout.write(Fore.BLUE + f" {mapValue} " + Style.RESET_ALL)
                else: # Clientes
                    sys.stdout.write(Fore.YELLOW + f" {mapValue} " + Style.RESET_ALL)

            else:
                sys.stdout.write(f" {mapValue} ")

        sys.stdout.write("\n")  

    sys.stdout.write(LINE)
    sys.stdout.write(f"{' ':<16} Estado general del sistema: OK\n")
    sys.stdout.write(LINE)

    sys.stdout.flush()  # Asegurar que todo se envía a la consola
###############################################################################################################

# Función que recibe las incidencias desde Kafka
def recibir_estado_taxi():
    consumer = kafka.KafkaConsumer('InfoEstadoTaxi', bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], group_id="centralGroup")

    for msg in consumer:
        msg = msg.value.decode(FORMAT)
        idTaxi = msg.split(", ")[0]
        status = msg.split(", ")[1]
        
        if status == "KO":
            print(f"[CENTRAL] Estado Taxi recibido: {msg}")

# Función encargada de recibir el movimiento del taxi
def recibir_movimiento_taxi():
    consumer = kafka.KafkaConsumer('TaxiMovimiento', bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], group_id="centralGroup")

    for mensaje in consumer:
        mensj = mensaje.value.decode(FORMAT)
        print(f"[CENTRAL] Movimiento recibido: {mensj}")

        # Actualiza el mapa y muestra la nueva posición
        actualizar_mapa_con_movimiento(mensj)

# Localizaciones de EC_locations.json
def loadLocations():
    global mapa

    try:
        with open("EC_locations.json", "r", encoding="utf-8") as locats:
            x = json.load(locats)
            
            for location in x["locations"]:
                locationID = location["Id"]
                locationPos = location["POS"]
                locationPosX = int(locationPos.split(",")[0])
                locationPosY = int(locationPos.split(",")[1])

                locations.append((locationID, locationPosX, locationPosY))

                mapa[locationPosX-1][locationPosY-1].pop(0)
                mapa[locationPosX-1][locationPosY-1].insert(0, locationID)
    
    except Exception:
        print(F"NO EXISTE EL ARCHIVO (EC_locations.json)")

# Poner a un consumidor en una localización
def putCustomerLocation(customerID, locationID):
    for location in locations:
        if location[0] == locationID:
            locationPosX = location[1]
            locationPosY = location[2]

            mapa[locationPosX-1][locationPosY-1].insert(0, customerID)

            return True
        
    return False

def freePositionMap(id):
    for row in range(len(mapa)):
        for col in range(len(mapa[row])):
            if mapa[row][col][0] == id:
                if len(mapa[row][col]) > 1:
                    mapa[row][col].pop(0) # Cambia la posición anterior a posición vacía

# Función encargada de actualizar el mapa
def actualizar_mapa_con_movimiento(mensaje):
    global mapa
    partes = mensaje.split(": ")
    id_taxi = int(partes[0].split(" ")[1].strip())  
    direccion = partes[1].split(", ")[0].strip()
    posicion = partes[2].strip() 
    x, y = map(int, [coord.split("=")[1] for coord in posicion.split(", ")])

    # Ajustar las coordenadas de 1-20 a 0-19 para usar en la matriz `mapa`
    x -= 1
    y -= 1

    # Limpiar la posición anterior del taxi
    for row in range(len(mapa)):
        for col in range(len(mapa[row])):
            if isinstance(mapa[row][col][0], int) and mapa[row][col][0] == id_taxi:
                if len(mapa[row][col]) > 1:
                    mapa[row][col].pop(0) # Cambia la posición anterior a posición vacía

    # Actualiza la nueva posición en el mapa según la dirección
    if 0 <= x < len(mapa) and 0 <= y < len(mapa[0]):
        if direccion in ["Norte", "Sur", "Este", "Oeste", "Noreste", "Sureste", "Noroeste", "Suroeste"]:
            # Guarda el número del taxi en el mapa para ser coloreado en la función mostrar_mapa
            mapa[x][y].insert(0, id_taxi)
        else:
            print(f"[ERROR] Dirección no reconocida: {direccion}")

    # Muestra el mapa actualizado
    mostrar_mapa()

# Gestionar los taxis
CLEAN = 1 # Deshabilitar los no activos (y desconecta el cliente que podría tener asociado)
CONNECT2TAXI = 2 # Añadir un servicio (cliente) a un taxi
DISCONNECT = 3 # Liberar un taxi de su servicio (cliente)
def manageTaxi(option, idCustomer=0, destination=0):
    modifiedLine = []
    modified = False
    customer2Disconnect = 0
    with open("database.txt", "r+") as file:
        for line in file:
            id = int(line.split(",")[0]) # 1 (ejemplo)
            destinationTaxi = line.split(",")[1] # - (ejemplo)
            status = line.split(",")[2] # OK.Parado (ejemplo)
            active = status.split(".")[0] # OK (ejemplo)
            service = status.split(".")[1] # Parado (ejemplo)

            if option == 1 and (id not in taxis): # Necesita option
                if service.startswith("Servicio"):
                    customer2Disconnect = (service.split(" ")[1]).split("\n")[0]

                line = line.replace(destinationTaxi, "-")
                line = line.replace(status, "NO.Parado\n")

            if option == 2 and (destinationTaxi == "-" and active == "OK"): # Necesita option, idCustomer, destination
                modified = True
                line = line.replace("-", destination)
                line = line.replace("Parado", f"Servicio {idCustomer}")

            if option == 3 and (service == f"Servicio {idCustomer}\n"): # Necesita option, idCustomer
                line = line.replace(destinationTaxi, "-")
                line = line.replace(status, "OK.Parado\n")
                
            modifiedLine.append(line)
        
        file.seek(0)
        file.writelines(modifiedLine)
        file.truncate()

    return modified, customer2Disconnect

# Obtener todos los datos de un taxi según su ID
def getTaxi(idTaxi):
    with open("database.txt", "r") as file:
        for line in file:
            id = int(line.split(",")[0]) # 1 (ejemplo)
            
            if id == idTaxi:
                destinationTaxi = line.split(",")[1] # - (ejemplo)
                status = line.split(",")[2] # OK.Parado (ejemplo)
                active = status.split(".")[0] # OK (ejemplo)
                service = status.split(".")[1] # Parado (ejemplo)

                if active == "OK":
                    return (id, destinationTaxi, active, service)

# Buscar y activar un taxi por su ID
def searchTaxiID(idTaxi):
    exists = False
    with open("database.txt", "r+") as file:
        modifiedLine = []

        for line in file:
            id = int(line.split(",")[0])

            if idTaxi == id and idTaxi not in taxis: # Si se encuentra esa ID en la base de datos y no es un taxi activo ya
                exists = True
                taxis.append(idTaxi)
                line = line.replace("NO", "OK")
                
            modifiedLine.append(line)
        
        file.seek(0)
        file.writelines(modifiedLine)
        file.truncate()

    return exists

# Se autentica el taxi buscándolo en la base de datos e envía un mensaje
def authTaxi(conn):
    try:
        msgTaxi = conn.recv(HEADER).decode(FORMAT)
        idTaxi = int(msgTaxi.split("#")[1])

        msg2Send = f"VERIFICANDO SOLICITUD DEL TAXI {idTaxi}...\nVERIFICACIÓN NO SUPERADA."
        if searchTaxiID(idTaxi):
            msg2Send = f"VERIFICANDO SOLICITUD DEL TAXI {idTaxi}...\nVERIFICACIÓN SUPERADA."
            mostrarTaxisCustomers()

        print(msg2Send)
        conn.send(msg2Send.encode(FORMAT))
        conn.close()

    except ConnectionResetError:
        # print(f"ERROR!! DESCONEXIÓN DEL TAXI {idTaxi} NO ESPERADA.")
        taxis.remove(idTaxi)

# Abre un socket para aceptar peticiones de autenticación
def connectionSocket(server):
    server.listen()
    print(f"CENTRAL A LA ESCUCHA EN {server}")
    while True:
        conn, addr = server.accept()
        
        threadAuthTaxi = threading.Thread(target=authTaxi, args=(conn,))
        threadAuthTaxi.start()

# ENVIAR UN MENSAJE A TRAVÉS DE KAFKA
# Crear un productor y enviar un mensaje a través de Kafka con un topic y su mensaje
def sendMessageKafka(topic, msg):
    time.sleep(0.1)
    PRODUCER.send(topic, msg.encode(FORMAT))
    PRODUCER.flush()

# CUSTOMERS
# Leer todas las solicitudes de los clientes
def requestCustomers():
    consumer = kafka.KafkaConsumer("Customer2Central", group_id="customer2CentralGroup", bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])
    for msg in consumer:
        message = msg.value.decode(FORMAT)

        if message == "ACTIVE?":
            sendMessageKafka("Central2Customer", "CENTRAL ACTIVE")
            continue

        id = message.split(" ")[0]
        ubicacion = message.split(" ")[1]
        destino = message.split(" ")[2]

        for customer in customers:
            if customer[0] == id:
                customers.remove(customer)

        isConnected, _ = manageTaxi(CONNECT2TAXI, id, ubicacion)
        if isConnected:
            customers.append((id, ubicacion, destino, "OK."))
            sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO ACEPTADO.")
            sendMessageKafka("Central2Taxi", f"{ubicacion} {destino}")
        
        elif not isConnected:
            customers.append((id, ubicacion, destino, "KO."))
            sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO RECHAZADO.")

        putCustomerLocation(id, ubicacion)
        mostrar_mapa()

# GESTIONA EL MANTENIMIENTO DE LOS CLIENTES O LOS TAXIS
# Deshabilitar los taxis o clientes no activos
def disableNoActives(activeTaxis, activeCustomers):
    for taxi in taxis:
        if taxi not in activeTaxis:
            sendMessageKafka("Central2Taxi", f"FIN {taxi}")
            print(f"DESCONEXIÓN DEL TAXI {taxi} NO ESPERADA.")
            freePositionMap(taxi)
            taxis.remove(taxi)
            _, customer2Disconnect = manageTaxi(CLEAN) # Limpiar taxis no activos
            sendMessageKafka("Central2Customer", f"CLIENTE {customer2Disconnect} SERVICIO RECHAZADO.")
            mostrar_mapa()
        
    for customer in customers:
        if customer[0] not in activeCustomers:
            sendMessageKafka("Central2Customer", f"FIN {customer[0]}")
            print(f"DESCONEXIÓN DEL CLIENTE {customer[0]} NO ESPERADA.")
            manageTaxi(DISCONNECT, customer[0]) # Desconectar cliente de taxi
            freePositionMap(customer[0])
            customers.remove(customer)
            mostrar_mapa()

# Comprobar que los taxis y customers están activos (10 segundos)
def areActives():
    while True:
        activeTaxis = []
        activeCustomers = []
        
        sendMessageKafka("Central2Taxi", "TAXI STATUS")
        sendMessageKafka("Central2Customer", "CUSTOMER STATUS")
        consumer = kafka.KafkaConsumer("Status", group_id="statusGroup", bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])

        startTime = time.time()
        while True:
            if time.time() - startTime > 9:
                break
            
            messages = consumer.poll(1000)
            for _, messagesValues in messages.items():
                for msg in messagesValues:
                    msg = msg.value.decode(FORMAT)
                    id = msg.split(" ")[1]

                    try:
                        taxiID = int(id)
                        activeTaxis.append(taxiID)

                    except Exception:
                        customerID = id
                        activeCustomers.append(customerID)

        disableNoActives(activeTaxis, activeCustomers)
        consumer.close()
        time.sleep(1)

def mostrarTaxisCustomers():
    print("\nTAXIS", end=": ")
    for taxi in taxis:
        print(taxi, end=", ")

    print("\nCLIENTES", end=": ")
    for customer in customers:
        print(customer, end=", ")

    print("\n")

# Consumidor ficticio para borrar mensajes
def fictitiousConsumer(topic, groupID):
    consumer = kafka.KafkaConsumer(topic, group_id=groupID, auto_offset_reset="latest", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

    startTime = time.time()
    while True:
        endTime = time.time()
        if endTime - startTime > 1:
            break
        
        messages = consumer.poll(10)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                x = 0
        
    consumer.close()

# Borrar mensajes acumulados en los topics
def deleteNoNecessaryMessages():
    topics = ["InfoEstadoTaxi", "TaxiMovimiento", "Customer2Central", "Status"]
    groupIDs = ["centralGroup", "centralGroup", "customer2CentralGroup", "statusGroup"]

    for i in range(len(topics)):
        fictitiousConsumer(topics[i], groupIDs[i])
    
    return True

# MAIN
def main(port):
    print("INICIANDO CENTRAL...")

    if deleteNoNecessaryMessages():
        server = socket.gethostbyname(socket.gethostname())

        addr = (server, int(port))
        
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(addr)

        print("CENTRAL INICIÁNDOSE...")

        manageTaxi(CLEAN)

        threadSockets = threading.Thread(target=connectionSocket, args=(server,))
        threadSockets.start()

        threadResquests = threading.Thread(target=requestCustomers)
        threadResquests.start()

        threadAreActives = threading.Thread(target=areActives)
        threadAreActives.start()


        threadInfoEstadoTaxi = threading.Thread(target=recibir_estado_taxi)
        threadInfoEstadoTaxi.start()

        # PARA MOSTRAR MAPA
        #######################################################################
        loadLocations()
        mostrar_mapa()

        threadMovimientoTaxi = threading.Thread(target=recibir_movimiento_taxi)
        threadMovimientoTaxi.start()
        ########################################################################

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        KAFKA_IP = sys.argv[2]
        KAFKA_PORT = int(sys.argv[3])
        PRODUCER = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

        main(sys.argv[1])

    else:
        print("ERROR! Se necesitan estos argumentos: <PUERTO DE ESCUCHA> <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER>")
