#region LIBRARIES
import sys
import socket
import threading
import kafka
import time
import colorama
from colorama import *
import json
import msvcrt
import uuid
import datetime
import ssl
import kafka.errors

#region CONSTANTS
HEADER = 64
FORMAT = 'utf-8'
END_CONNECTION1 = "FIN"
END_CONNECTION2 = "ERROR"
KAFKA_IP = 0
KAFKA_PORT = 0
PRODUCER = 0
SOCKET_IP = 0

# Constantes del mapa
CHAR_MAP = "."
SPACE = " "
LINE = f"{'-' * 64}\n"

#region MAP
mapa = [[["."] for _ in range(20)] for _ in range(20)]
taxis = []
customers = []
locations = []
mapa2Print = []

colorama.init(autoreset=True)

def mostrar_mapa():
    mapa2Print.clear()

    mapa2Print.append("\n" * 6)

    mapa2Print.append(LINE)
    mapa2Print.append(f"{' ':<15} *** EASY CAB Release 1 ***\n")
    mapa2Print.append(LINE)
    
    # Mostrar encabezado de taxis y clientes
    mapa2Print.append(f"{' ':<10} {'Taxis':<19} {'|':<8} {'Clientes'}\n")
    mapa2Print.append(LINE)
    mapa2Print.append(f"{' ':<3} {'Id.':<5} {'Destino':<10} {'Estado':<9} {'|':<2} {'Id.':<5} {'Destino':<10} {'Estado':<10}\n")
    mapa2Print.append(LINE)

    # Mostrar los taxis y los clientes en filas paralelas
    maxSize = max(len(taxis), len(customers))
    for i in range(maxSize):
        taxi_line = f"{SPACE * 3}"
        cliente_line = f"{SPACE * 3}"
        
        if i < len(taxis):
            taxi = getTaxi(ONE, taxis[i])
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

                taxi_line = f"{SPACE*4} {idTaxi} {SPACE*5} {destTaxi} {stateTaxi}  "

                if activeTaxi == "KO":
                    taxi_line = Fore.RED + taxi_line + Style.RESET_ALL
                
                taxi_line += "|"

            else:
                taxi_line = SPACE * 30
        
        if i < len(customers):
            customer = customers[i]
            idCustomer = customer[0]
            ubiCustomer = customer[1]
            destCustomer = customer[2]
            stateCustomer = customer[3]

            cliente_line = f"{idCustomer:<5} {destCustomer:<10} {stateCustomer}"
        
        mapa2Print.append(f"{taxi_line} {cliente_line}\n")

        if i == maxSize - 1:
            mapa2Print.append(LINE)


    mapa2Print.append(LINE)

    mapa2Print.append("   " + " ".join([f"{i:2}" for i in range(1, 21)]) + "\n")
    
    mapa2Print.append(LINE)

    showMapObjects(mapa2Print)

    mapa2Print.append(LINE)
    mapa2Print.append(f"{' ':<16} Estado general del sistema: OK\n")
    mapa2Print.append(LINE)

    fullMapa2Print = "".join(mapa2Print)
    print(fullMapa2Print)

    time.sleep(0.25)
    sendMessageKafka("Mapa", fullMapa2Print)

# Mostrar taxis, localizaciones, clientes y movimientos en el mapa
def showMapObjects(mapa2Print):
    for row in range(len(mapa)):
        # Mostrar el número de fila (ajustado de 1-20)
        mapa2Print.append(f"{row + 1:2} ")
        for col in range(len(mapa[row])):
            mapValue = mapa[row][col][0]

            #print(" ", end="")
            #print(mapa[row][col], end=" ")

            if True:
                if isinstance(mapValue, int):  # Taxis
                    taxi = getTaxi(ONE, mapValue)
                    destinationTaxi = taxi[1]
                    activeTaxi = taxi[2]
                    serviceTaxi = taxi[3]

                    if activeTaxi == "OK" and serviceTaxi.startswith("Servicio"):
                        idCustomer = serviceTaxi.split(" ")[1]

                        customer = searchCustomerID(idCustomer)

                        # Si el destino del TAXI coincide con el destino del cliente que esta en esa casilla se ponen juntos y, además, el ID del customer que hace el servicio el taxi debe coincidir con el ID del Customer que esta en esa posicion
                        if idCustomer in mapa[row][col] and destinationTaxi == customer[2]: 
                            mapa2Print.append(Fore.GREEN + f"{mapValue:2}{idCustomer}" + Style.RESET_ALL)
                        else:
                            mapa2Print.append(Fore.GREEN + f"{mapValue:2} " + Style.RESET_ALL)

                    elif activeTaxi == "OK" and serviceTaxi == "Parado":
                        mapa2Print.append(Fore.RED + f"{mapValue:2} " + Style.RESET_ALL)

                    elif activeTaxi == "KO":
                        mapa2Print.append(Fore.RED + f"{mapValue:2}!" + Style.RESET_ALL)

                elif isinstance(mapValue, str) and len(mapValue) == 1 and mapValue.isalpha():
                    if mapValue.isupper():  # Localizaciones
                        mapa2Print.append(Fore.BLUE + f" {mapValue} " + Style.RESET_ALL)
                    else: # Clientes
                        mapa2Print.append(Fore.YELLOW + f" {mapValue} " + Style.RESET_ALL)

                else:
                    mapa2Print.append(f" {mapValue} ")

        mapa2Print.append("\n")  

def freePositionMap(id):
    for row in range(len(mapa)):
        for col in range(len(mapa[row])):
            try:
                idIndex = mapa[row][col].index(id)
            except: 
                idIndex = -1

            if idIndex != -1 and mapa[row][col][idIndex] == id:
                if len(mapa[row][col]) > 1:
                    mapa[row][col].pop(idIndex) # Cambia la posición anterior a posición vacía

# Función encargada de actualizar el mapa
def actualizar_mapa_con_movimiento(mensaje):
    global mapa
    words = mensaje.split(" ")
    #print(mensaje)
    idTaxi = int(words[1])  
    direccion = words[3]
    x = int(words[5])
    y = int(words[6])

    #x, y = map(int, [coord.split("=")[1] for coord in posicion.split(", ")])

    # Ajustar las coordenadas de 1-20 a 0-19 para usar en la matriz `mapa`
    x -= 1
    y -= 1

    serviceCustomerID = -1
    # Limpiar la posición anterior del taxi
    for row in range(len(mapa)):
        for col in range(len(mapa[row])):
            try:
                taxiIndex = mapa[row][col].index(idTaxi)
            except:
                taxiIndex = -1

            if taxiIndex != -1 and isinstance(mapa[row][col][taxiIndex], int) and mapa[row][col][taxiIndex] == idTaxi:
                if len(mapa[row][col]) > 1:
                    #print(mapa[row][col][0])
                    #print(idTaxi)
                    #print(taxiIndex)
                    mapa[row][col].pop(taxiIndex) # Cambia la posición anterior a posición vacía

                    taxi = getTaxi(ONE, idTaxi)
                    destinationTaxi = taxi[1]
                    activeTaxi = taxi[2]
                    serviceTaxi = taxi[3]

                    if activeTaxi == "OK" and serviceTaxi.startswith("Servicio"):
                        idCustomer = serviceTaxi.split(" ")[1]
                        
                        customer = searchCustomerID(idCustomer)
                        
                        # Si el destino del TAXI coincide con el destino del cliente que esta en esa casilla se ponen juntos y, además, el ID del customer que hace el servicio el taxi debe coincidir con el ID del Customer que esta en esa posicion
                        if idCustomer in mapa[row][col] and destinationTaxi == customer[2] and len(mapa[row][col]) > 1:
                            #print(mapa[row][col])
                            customerIndex = mapa[row][col].index(customer[0])
                            serviceCustomerID = mapa[row][col][customerIndex]
                            mapa[row][col].pop(customerIndex) # Cambia la posición anterior a posición vacía
                            
                            if customer[3] != "OK":
                                i = 0
                                for customer in customers:
                                    if customer[0] == idCustomer:
                                        customers[i] = (customer[0], customer[1], customer[2], "OK")

                                    i += 1
                            

    # Actualiza la nueva posición en el mapa según la dirección
    if 0 <= x < len(mapa) and 0 <= y < len(mapa[0]):
        if direccion in ["Norte", "Sur", "Este", "Oeste", "Noreste", "Sureste", "Noroeste", "Suroeste"]:
            # Guarda el número del taxi en el mapa para ser coloreado en la función mostrar_mapa
            if serviceCustomerID != -1 and serviceCustomerID not in mapa[x][y]:
                mapa[x][y].insert(0, serviceCustomerID)
            
            if idTaxi not in mapa[x][y]:
                mapa[x][y].insert(0, idTaxi)

        else:
            print(f"[ERROR] Dirección no reconocida: {direccion}")

    # Muestra el mapa actualizado
    mostrar_mapa()

#region KAFKA
# ENVIAR UN MENSAJE A TRAVÉS DE KAFKA
# Crear un productor y enviar un mensaje a través de Kafka con un topic y su mensaje
def sendMessageKafka(topic, msg):
    time.sleep(0.25)
    PRODUCER.send(topic, msg.encode(FORMAT))
    PRODUCER.flush()

#region AUDIT LOGS
# Escribir en el archivo de logs de auditoria
def writeInAuditLog(action, description, IP=None):
    if IP is None:
        IP = socket.gethostbyname(socket.gethostname())
    
    # Obtener fecha actual y escribir el string del log
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logStr = f"[{timestamp}] IP={IP} ACTION={action} DESCRIPTION={description}\n"
    
    try:
        with open("audit.log", "a") as logFile:
            logFile.write(logStr)
    except Exception as e:
        print(f"ERROR AL ESCRIBIR EN EL ARCHIVO DE LOGS: {e}")

#region LOCATIONS CONTROL
# Obtener una localización según las coordenadas
def getLocationByCoords(x, y):
    for location in locations:
        if location[1] == x and location[2] == y:
            return location

    return None

# Obtener una localización según la ID de una localización
def getLocation(customerLocation):
    for location in locations:
        if location[0] == customerLocation:
            return (location[1], location[2])
        
    return None

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

#region CUSTOMERS CONTROL
# Poner a un consumidor en una localización
def putCustomerLocation(customerID, locationID):
    for location in locations:
        if location[0] == locationID:
            locationPosX = location[1]
            locationPosY = location[2]

            if customerID not in mapa[locationPosX-1][locationPosY-1]:
                mapa[locationPosX-1][locationPosY-1].insert(0, customerID)

            return True
        
    return False

# Buscar un CUSTOMER según su ID
def searchCustomerID(idCustomer):
    for customer in customers:
        if customer[0] == idCustomer:
            return customer
        
    return -1

# Leer todas las solicitudes de los clientes
def requestCustomers():
    consumer = kafka.KafkaConsumer("Customer2Central", group_id=str(uuid.uuid4()), bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])
    for msg in consumer:
        msg = msg.value.decode(FORMAT)

        if msg.startswith("SOLICITUD DE SERVICIO:"):
            #print(f"Holaaaaaaaaaa: {msg}")
            id = msg.split(" ")[3]
            ubicacion = msg.split(" ")[4]
            destino = msg.split(" ")[5]

            for customer in customers:
                if customer[0] == id:
                    customers.remove(customer)
                    freePositionMap(customer[0])

            isConnected, _, idTaxi = manageTaxi(CONNECT2TAXI, id, ubicacion)
            if isConnected:
                customers.append((id, ubicacion, destino, f"OK. Taxi {idTaxi}"))
                sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO ACEPTADO.")
                
                locationTuple = getLocation(ubicacion)
                x = locationTuple[0]
                y = locationTuple[1]
                #print(locationTuple)
                sendMessageKafka("Central2Taxi", f"TAXI {idTaxi} DESTINO {x},{y} CLIENTE {id}")
            
            elif not isConnected:
                customers.append((id, ubicacion, destino, "KO. Servicio denegado."))
                sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO RECHAZADO.")
                freePositionMap(id)

            putCustomerLocation(id, ubicacion)
            mostrar_mapa()

#region TAXI CONTROL
# Gestionar los taxis
CLEAN = 1 # Deshabilitar los no activos (y desconecta el cliente que podría tener asociado)
def manageTaxiCLEAN(line, id, service, destinationTaxi, status):
    customer2Disconnect = 0
    if id not in taxis:
        if service.startswith("Servicio"):
            customer2Disconnect = (service.split(" ")[1]).split("\n")[0]

        line = line.replace(destinationTaxi, "-")
        line = line.replace(status, "NO.Parado\n")

    return line, customer2Disconnect

CONNECT2TAXI = 2 # Añadir un servicio (cliente) a un taxi
CONNECT2TAXI_ID = 2.1 # Añadir un servicio (central) a un taxi en concreto
def manageTaxiCONNECT2TAXI(line, option, id, idTaxi, destinationTaxi, active, destination, idCustomer, modified, modifiedTaxiID):
    if CONNECT2TAXI == option and not modified and destinationTaxi == "-" and active == "OK":
        for location in locations:
            if location[0] == destination:
                modified = True
                modifiedTaxiID = id
                line = line.replace("-", destination)
                line = line.replace("Parado", f"Servicio {idCustomer}")
                writeInAuditLog("TAXI_CUSTOMER_SERVICE", f"AL TAXI {idTaxi} SE LE HA ASIGNADO UN SERVICIO DE UN CLIENTE", SOCKET_IP)
                break

    elif CONNECT2TAXI_ID == option and idTaxi == id and destinationTaxi == "-" and active == "OK":
        if idCustomer == "Central":
            modified = True
            modifiedTaxiID = id
            line = line.replace("-", destination)
            line = line.replace("Parado", f"Servicio {idCustomer}")
            writeInAuditLog("TAXI_CENTRAL_SERVICE", f"AL TAXI {idTaxi} SE LE HA ASIGNADO UN SERVICIO DE LA CENTRAL", SOCKET_IP)

    return line, modified, modifiedTaxiID

DISCONNECT = 3 # Liberar un taxi de su servicio (cliente) debido a un error
DISCONNECT_COMPLETED = 3.1 # Liberar un taxi de su servicio (cliente) por servicio completado
def manageTaxiDISCONNECT(line, option, id, service, idCustomer, destinationTaxi, status):
    if service == f"Servicio {idCustomer}\n":
        line = line.replace(destinationTaxi, "-")
        line = line.replace(status, "OK.Parado\n")

        if DISCONNECT == option:
            sendMessageKafka("Central2Taxi", f"TAXI {id} ERROR CUSTOMER")
        elif DISCONNECT_COMPLETED == option:
            sendMessageKafka("Central2Customer", f"CLIENTE {idCustomer} SERVICIO COMPLETADO.")
            writeInAuditLog("TAXI_CUSTOMER_SERVICE_COMPLETED", f"EL TAXI {id} HA COMPLETADO EL SERVICIO DEL CLIENTE {idCustomer}", SOCKET_IP)

    return line

CHANGE_DESTINATION = 4 # Cambiar el destino de un taxi
def manageTaxiCHANGE_DESTINATION(line, id, idTaxi, destinationTaxi, destination):
    if id == idTaxi:
        #print(idTaxi)
        line = line.replace(destinationTaxi, destination)
        writeInAuditLog("TAXI_CUSTOMER_PICKUP", f"EL TAXI {idTaxi} HA RECOGIDO AL CLIENTE ASIGNADO", SOCKET_IP)

    return line

OK = 5 # OK en taxi
def manageTaxiOK(line, id, idTaxi, active):
    if id == idTaxi and active == "KO":
        line = line.replace(active, "OK")

    return line

KO = 6 # KO en taxi
def manageTaxiKO(line, id, idTaxi, active):
    if id == idTaxi and active == "OK":
        line = line.replace(active, "KO")

    return line

def manageTaxi(option, idCustomer = -1, destination = -1, idTaxi = -1):
    modifiedLine = []
    modified = False
    modifiedTaxiID = 0
    customer2Disconnect = 0
    with open("database.txt", "r+") as file:
        for line in file:
            id = int(line.split(",")[0]) # 1 (ejemplo)
            destinationTaxi = line.split(",")[1] # - (ejemplo)
            status = line.split(",")[2] # OK.Parado (ejemplo)
            active = status.split(".")[0] # OK (ejemplo)
            service = status.split(".")[1] # Parado (ejemplo)

            if option == CLEAN:
                line, customer2Disconnect = manageTaxiCLEAN(line, id, service, destinationTaxi, status)

            elif option == CONNECT2TAXI or option == CONNECT2TAXI_ID:
                line, modified, modifiedTaxiID = manageTaxiCONNECT2TAXI(line, option, id, idTaxi, destinationTaxi, active, destination, idCustomer, modified, modifiedTaxiID)

            elif option == DISCONNECT or option == DISCONNECT_COMPLETED:
                line = manageTaxiDISCONNECT(line, option, id, service, idCustomer, destinationTaxi, status)

            elif option == CHANGE_DESTINATION:
                line = manageTaxiCHANGE_DESTINATION(line, id, idTaxi, destinationTaxi, destination)

            elif option == OK:
                line = manageTaxiOK(line, id, idTaxi, active)

            elif option == KO:
                line = manageTaxiKO(line, id, idTaxi, active)
                
            modifiedLine.append(line)
        
        file.seek(0)
        file.writelines(modifiedLine)
        file.truncate()

    return modified, customer2Disconnect, modifiedTaxiID

# Obtener todos los datos de un taxi o todos los taxis
ONE = 1 # Obtener solo 1 taxi
ALL = 2 # Obtener todos los taxis activos de la base de datos
def getTaxi(option, idTaxi = 0):
    activeTaxis = []
    with open("database.txt", "r") as file:
        for line in file:
            id = int(line.split(",")[0]) # 1 (ejemplo)
            status = line.split(",")[2] # OK.Parado (ejemplo)
            active = status.split(".")[0] # OK (ejemplo)
            
            if option == ONE and (id == idTaxi):
                destinationTaxi = line.split(",")[1] # - (ejemplo)
                status = line.split(",")[2] # OK.Parado (ejemplo)
                active = status.split(".")[0] # OK (ejemplo)
                service = status.split(".")[1].split("\n")[0] # Parado (ejemplo)
                return (id, destinationTaxi, active, service)
            
            if option == ALL and (active == "OK" or active == "KO"):
                activeTaxis.append((id, active))

    return activeTaxis

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

# Recibe inicidencias y movimientos
def receiveInfoTaxi():
    consumer = kafka.KafkaConsumer("Taxi2Central", bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], group_id=str(uuid.uuid4()))

    beforeStatus = {}
    for msg in consumer:
        msg = msg.value.decode(FORMAT)
        
        if msg.split(" ")[2] == "MOVIMIENTO":
            actualizar_mapa_con_movimiento(msg)

        elif msg.split(" ")[2] == "DESTINO":
            taxiID = int(msg.split(" ")[1])
            destinoCoord = msg.split(" ")[3].split(",")
            destino = (int(destinoCoord[0]), int(destinoCoord[1]))
            customerID = msg.split(" ")[5]
            #print(msg)

            if customerID == "CENTRAL":
                manageTaxi(DISCONNECT_COMPLETED, idCustomer="Central")
                mostrar_mapa()

            else:
                destinoTaxi = getLocationByCoords(destino[0], destino[1])
                for customer in customers:
                    if customer[0] == customerID:
                        if destinoTaxi[0] == customer[1]:
                            destinoTuple = getLocation(customer[2])
                            x = destinoTuple[0]
                            y = destinoTuple[1]
                            #print(f"TAXI {taxiID} DESTINO {x},{y} CLIENTE {customerID}")
                            sendMessageKafka("Central2Taxi", f"TAXI {taxiID} DESTINO {x},{y} CLIENTE {customerID}")
                            manageTaxi(CHANGE_DESTINATION, destination=customer[2], idTaxi=taxiID)
                        
                        elif destinoTaxi[0] == customer[2]:
                            manageTaxi(DISCONNECT_COMPLETED, customer[0]) # Desconectar cliente de taxi
                            mostrar_mapa()

        else:
            idTaxi = int(msg.split(" ")[1])
            status = msg.split(" ")[2]

            if idTaxi in beforeStatus and status != beforeStatus[idTaxi]:
                if beforeStatus[idTaxi] == "KO":
                    manageTaxi(OK, 0, 0, idTaxi)
                else:
                    manageTaxi(KO, 0, 0, idTaxi)
                
                mostrar_mapa()

            if status in ["OK", "KO"]:
                for key in list(beforeStatus.keys()):
                    if key not in taxis:
                        del beforeStatus[idTaxi]

                beforeStatus[idTaxi] = status

# Se autentica el taxi buscándolo en la base de datos e envía un mensaje
def authTaxi(conn):
    try:
        msgTaxi = conn.recv(HEADER).decode(FORMAT)
        idTaxi = int(msgTaxi.split("#")[1])

        writeInAuditLog("AUTH_ATTEMPT", f"EL TAXI {idTaxi} ESTA INTENTANDO AUTENTICARSE", SOCKET_IP)
        msg2Send = f"VERIFICANDO SOLICITUD DEL TAXI {idTaxi}...\nVERIFICACIÓN NO SUPERADA."
        if searchTaxiID(idTaxi):    
            msg2Send = f"VERIFICANDO SOLICITUD DEL TAXI {idTaxi}...\nVERIFICACIÓN SUPERADA."
            writeInAuditLog("AUTH_SUCCESSFUL", f"EL TAXI {idTaxi} SE HA AUTENTICADO CORRECTAMENTE", SOCKET_IP)
            if idTaxi not in mapa[0][0]:
                mapa[0][0].insert(0, idTaxi)
        else:
            writeInAuditLog("AUTH_REJECTED", f"EL TAXI {idTaxi} SE HA RECHAZADO LA AUTENTICACION", SOCKET_IP)

        print(msg2Send)
        conn.send(msg2Send.encode(FORMAT))

        mostrar_mapa()

    except ConnectionResetError:
        # print(f"ERROR!! DESCONEXIÓN DEL TAXI {idTaxi} NO ESPERADA.")
        taxis.remove(idTaxi)

    except Exception:
        text = "ID DEL TAXI NO ENCONTRADA EN LA BASE DE DATOS\nVERIFICACIÓN NO SUPERADA."
        print(text)
        conn.send(text.encode(FORMAT))
        writeInAuditLog("AUTH_REJECTED", f"EL TAXI {idTaxi} SE HA RECHAZADO LA AUTENTICACION", SOCKET_IP)

    finally:
        conn.close()

# Abre un socket para aceptar peticiones de autenticación con SSL/TLS
def connectionSocketSSL(server):
    # Crear contexto SSL para el servidor
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="central.crt", keyfile="central.key")
    context.load_verify_locations("ca.pem")

    # Crear el socket del servidor
    #server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #server.bind(("127.0.0.1", 5050))
    server.listen(5)
    #print(f"CENTRAL A LA ESCUCHA EN 127.0.0.1:5050 (SSL/TLS habilitado)")

    while True:
        # Aceptar conexiones entrantes
        client_socket, addr = server.accept()

        # Envolver el socket con SSL
        ssl_client_socket = context.wrap_socket(client_socket, server_side=True)
        print(f"CONEXIÓN SSL ESTABLECIDA CON {addr}")

        # Crear un hilo para manejar la autenticación del taxi
        threadAuthTaxi = threading.Thread(target=authTaxi, args=(ssl_client_socket,))
        threadAuthTaxi.start()

# Abre un socket para aceptar peticiones de autenticación con SSL/TLS
def connectionSocket(server):
    # Crear contexto SSL para el servidor
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="central.crt", keyfile="central.key")
    context.load_verify_locations("ca.pem")

    # Crear el socket del servidor
    #server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #server.bind(("127.0.0.1", 5050))
    server.listen(5)
    #print(f"CENTRAL A LA ESCUCHA EN 127.0.0.1:5050 (SSL/TLS habilitado)")

    while True:
        # Aceptar conexiones entrantes
        client_socket, addr = server.accept()

        # Envolver el socket con SSL
        #ssl_client_socket = context.wrap_socket(client_socket, server_side=True)
        print(f"CONEXIÓN SSL ESTABLECIDA CON {addr}")

        # Crear un hilo para manejar la autenticación del taxi
        threadAuthTaxi = threading.Thread(target=authTaxi, args=(addr,))
        threadAuthTaxi.start()

#region ACTIVITY CONTROL
# GESTIONA EL MANTENIMIENTO DE LOS CLIENTES O LOS TAXIS
def centralActive():
    while True:
        sendMessageKafka("Central2Customer", "CENTRAL ACTIVE")
        sendMessageKafka("Central2Taxi", "CENTRAL ACTIVE")
        time.sleep(1)

# Deshabilitar los taxis o clientes no activos
def disableNoActives(activeTaxis, activeCustomers):
    actualTaxis = taxis
    actualCustomers = customers

    for taxi in actualTaxis:
        if taxi not in activeTaxis:
            writeInAuditLog("TAXI_DISCONNECTED", f"EL TAXI {taxi} SE HA DESCONECTADO.")
            sendMessageKafka("Central2Taxi", f"FIN {taxi}")
            print(f"DESCONEXIÓN DEL TAXI {taxi} NO ESPERADA.")
            freePositionMap(taxi)
            taxis.remove(taxi)
            _, customer2Disconnect, _ = manageTaxi(CLEAN) # Limpiar taxis no activos
            sendMessageKafka("Central2Customer", f"CLIENTE {customer2Disconnect} SERVICIO RECHAZADO.")
            mostrar_mapa()
        
    for customer in actualCustomers:
        if customer[0] not in activeCustomers:
            writeInAuditLog("CUSTOMER_DISCONNECTED", f"EL CLIENTE {customer[0]} SE HA DESCONECTADO.")
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

        consumer = kafka.KafkaConsumer("Status", group_id=str(uuid.uuid4()), bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])

        startTime = time.time()
        while True:
            if time.time() - startTime > 11:
                break
            
            messages = consumer.poll(1000)
            for _, messagesValues in messages.items():
                for msg in messagesValues:
                    msg = msg.value.decode(FORMAT)
                    id = msg.split(" ")[1]

                    try:
                        taxiID = int(id)
                        if taxiID not in activeTaxis:
                            activeTaxis.append(taxiID)

                    except Exception:
                        customerID = id
                        if customerID not in activeCustomers:
                            activeCustomers.append(customerID)

        disableNoActives(activeTaxis, activeCustomers)
        consumer.close()
        time.sleep(1)

#region KEYS
# Detectar dos teclas o 3 teclas seguidas
def detectKeys():
    if msvcrt.kbhit():
        firstKey = msvcrt.getch().decode(FORMAT).lower()

        while True:
            if msvcrt.kbhit():
                secondKey = msvcrt.getch().decode(FORMAT).lower()
            
                if secondKey == "d":
                    while True:
                        if msvcrt.kbhit():
                            thirdKey = msvcrt.getch().decode(FORMAT)
                            # Devolvemos las tres teclas concatenadas en el caso que tengamos que enviarlo a un destino
                            return firstKey + secondKey + thirdKey
                
                # Devolvemos las dos teclas concatenadas
                return firstKey + secondKey

# Gestionar acciones desde al Central a un Taxi
def taxiKeys():
    while True:
        detectedKeys = detectKeys()

        if detectedKeys is not None:
            try:
                idTaxi = detectedKeys[0]

                if int(idTaxi) not in taxis:
                    print(f"NO EXISTE EL TAXI {idTaxi}")
            
                else:
                    idTaxi = int(idTaxi)

                    actionTaxiKey = detectedKeys[1]
                    actionTaxi = "" # (idTaxi)p - Parar ### (idTaxi)r - Reanudar ### (idTaxi)d - Destino ### (idTaxi)b - Base
                    if actionTaxiKey == "p":
                        actionTaxi = "PARAR"
                    elif actionTaxiKey == "r":
                        actionTaxi = "REANUDAR"
                    elif actionTaxiKey == "d":
                        destino = None
                        for location in locations:
                            if location[0] == detectedKeys[2]:
                                destino = detectedKeys[2]
                        
                        if destino is not None:
                            isConnected, _, _ = manageTaxi(CONNECT2TAXI_ID, idCustomer="Central", destination=detectedKeys[2], idTaxi=idTaxi)
                            if isConnected:
                                locationTuple = getLocation(destino)
                                x = locationTuple[0]
                                y = locationTuple[1]
                                sendMessageKafka("Central2Taxi", f"TAXI {idTaxi} DESTINO {x},{y} CLIENTE CENTRAL")

                        else:
                            print(f"NO EXISTE LA LOCALIZACIÓN {detectedKeys[2]}")

                    elif actionTaxiKey == "b":
                        isConnected, _, _ = manageTaxi(CONNECT2TAXI_ID, idCustomer="Central", destination="Base", idTaxi=idTaxi)
                        if isConnected:
                            sendMessageKafka("Central2Taxi", f"TAXI {idTaxi} DESTINO 1,1 CLIENTE CENTRAL")
                        

                    sendMessageKafka("Central2Taxi", f"TAXI {idTaxi},CENTRAL {actionTaxi}")

            except Exception:
                print(f"NO EXISTE EL TAXI {idTaxi}")

        time.sleep(0.25)

#region MAIN
def main(port):
    print("INICIANDO CENTRAL...")

    # Sockets
    server = socket.gethostbyname(SOCKET_IP)
    addr = (server, int(port))
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(addr)

    # Gestión de taxis e hilos
    # manageTaxi(CLEAN)
    loadLocations()

    threadSockets = threading.Thread(target=connectionSocket, args=(server,))
    threadResquests = threading.Thread(target=requestCustomers)
    threadAreActives = threading.Thread(target=areActives)
    threadCentralActive = threading.Thread(target=centralActive)
    threadTaxiKeys = threading.Thread(target=taxiKeys)
    threadMovimientoTaxi = threading.Thread(target=receiveInfoTaxi)
    threads = [threadSockets, threadResquests, threadAreActives, threadTaxiKeys, threadMovimientoTaxi, threadCentralActive]

    for thread in threads:
        thread.start()

    mostrar_mapa()

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 5):
        SOCKET_IP = sys.argv[1]
        KAFKA_IP = sys.argv[3]
        KAFKA_PORT = int(sys.argv[4])
        PRODUCER = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

        main(sys.argv[2])

    else:
        print("ERROR! Se necesitan estos argumentos: <IP DE ESCUCHA> <PUERTO DE ESCUCHA> <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER>")
