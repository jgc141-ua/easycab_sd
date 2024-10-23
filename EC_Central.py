import sys
import socket
import threading
import kafka
import signal
import time

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

# PARA MOSTRAR MAPA
#####################################################
mapa = [["x" for _ in range(20)] for _ in range(20)]
taxis = []
customers = []
localizaciones = []
#####################################################

# PARA MOSTRAR MAPA
##############################
colorama.init(autoreset=True)
##############################

def centralDown(sig, frame):
    sendMessageKafka("Central2Taxi", "DEATH CENTRAL")
    sendMessageKafka("Central2Customer", "DEATH CENTRAL")
    sys.exit(0)

signal.signal(signal.SIGINT, centralDown)
signal.signal(signal.SIGTERM, centralDown)

# PARA MOSTRAR MAPA
def getTaxis():
    activeTaxis = []
    with open("database.txt", "r") as file:
        for line in file:
            id = int(line.split(",")[0].strip())
            destino = line.split(",")[1].strip()
            estado = line.split(",")[2].strip()

            activo = estado.split(".")[0].strip()
            servicio = estado.split(".")[1].strip()
            
            if activo != "NO":
                activeTaxis.append((id, destino, activo, servicio))

    return activeTaxis

# PARA MOSTRAR MAPA
def logica(activeTaxis):
    sizeTaxis = len(activeTaxis)
    sizeCustomers = len(customers)
    space = " "

    line = ""
    maxSize = max(sizeTaxis, sizeCustomers)
    for i in range(maxSize):
        if i < sizeTaxis:
            taxi = activeTaxis[i]
            line += f"|  {taxi[0]}{space * 5} {taxi[1]}{space * 8} {taxi[2]}{space * 8}"
        
        else:
            line += "|" + (space * 29)

        if i < sizeCustomers:
            customer = customers[i]
            line += f"|  {customer[0]}{space * 5} {customer[1]}{space * 8} {customer[2]}{space * 8}|\n"
        
        else:
            line += f"|{space * 30}|\n"

    line += " " + "-" * 60 + "\n"

    return line

# PARA MOSTRAR MAPA
###############################################################################################################
def mostrar_mapa(mapa, taxis, clientes):
    sys.stdout.write("------------------------------------------------------------\n")
    sys.stdout.write(f"{' ':<15} *** EASY CAB Release 1 ***\n")
    sys.stdout.write("------------------------------------------------------------\n")
    
    # Mostrar encabezado de taxis y clientes
    sys.stdout.write(f"{' ':<10} {'Taxis':<18} {'|':<8} {'Clientes'}\n")
    sys.stdout.write("------------------------------------------------------------\n")
    sys.stdout.write(f"{' ':<3} {'Id.':<5} {'Destino':<10} {'Estado':<8} {'|':<2} {'Id.':<5} {'Destino':<10} {'Estado':<10}\n")
    sys.stdout.write("------------------------------------------------------------\n")

    # Mostrar los taxis y los clientes en filas paralelas
    for i in range(max(len(taxis), len(clientes))):
        taxi_line = ""
        cliente_line = ""
        
        if i < len(taxis):
            taxi = taxis[i]
            taxi_line = f"{taxi['id']:<5} {taxi['destino']:<10} {taxi['estado']:<10}"
        else:
            taxi_line = " " * 25
        
        if i < len(clientes):
            cliente = clientes[i]
            cliente_line = f"{cliente['id']:<5} {cliente['destino']:<10} {cliente['estado']:<10}"
        else:
            cliente_line = " " * 25
        
        sys.stdout.write(f"{taxi_line} {cliente_line}\n")

    sys.stdout.write("------------------------------------------------------------\n")

    # Mostrar el mapa utilizando streams
    for row in range(len(mapa)):
        for col in range(len(mapa[row])):
            if isinstance(mapa[row][col], int):  # Taxis
                sys.stdout.write(Fore.GREEN + f" {mapa[row][col]} ")
            elif mapa[row][col] == 'C':  # Clientes
                sys.stdout.write(Fore.BLUE + f" C ")
            else:
                sys.stdout.write(f" X ")
        sys.stdout.write("\n")  # Nueva línea para cada fila del mapa

    sys.stdout.write("------------------------------------------------------------\n")
    sys.stdout.write(f"{' ':<12} Estado general del sistema: OK\n")
    sys.stdout.write("------------------------------------------------------------\n")

    sys.stdout.flush()  # Asegurar que todo se envía a la consola
###############################################################################################################

# Buscar un taxi por su ID y resetea los taxis que no se encuentren activos
def searchTaxiID(idTaxi):
    modifiedLine = []
    exists = False
    with open("database.txt", "r+") as file:
        for line in file:
            id = int(line.split(",")[0])
            if idTaxi == id and idTaxi not in taxis:
                exists = True
                line = line.replace("NO", "OK")

            else:
                if id not in taxis:
                    line = line.replace("OK", "NO")
                
            modifiedLine.append(line)
        
        file.seek(0)
        file.writelines(modifiedLine)
        file.truncate()

    return exists

def setTaxiDestination(destination, idCustomer):
    modifiedLine = []
    numLines = 0
    numNoModifiedLines = 0
    with open("database.txt", "r+") as file:
        for line in file:
            numLines += 1
            destinationTaxi = line.split(",")[1]
            status = line.split(",")[2]

            if destinationTaxi == "-" and status == "OK":
                line = line.replace("-", destination)
                line = line.replace("Parado", f"Servicio {idCustomer}")
                modifiedLine.append(line)
            
            else:
                numNoModifiedLines += 1
                modifiedLine.append(line)

        file.seek(0)
        file.writelines(modifiedLine)
        file.truncate()

    if numNoModifiedLines == numLines:
        return False

    return True

# Se autentica el taxi buscándolo en la base de datos e enía un mensaje
def authTaxi(conn):
    try:
        msgTaxi = conn.recv(HEADER).decode(FORMAT)
        idTaxi = int(msgTaxi.split("#")[1])

        msg2Send = f"VERIFICANDO SOLICITUD DEL TAXI {idTaxi}...\nVERIFICACIÓN NO SUPERADA."
        if searchTaxiID(idTaxi):
            msg2Send = f"VERIFICANDO SOLICITUD DEL TAXI {idTaxi}...\nVERIFICACIÓN SUPERADA."
            taxis.append(idTaxi)
            mapa[0][0] = idTaxi

        print(msg2Send)
        conn.send(msg2Send.encode(FORMAT))
        
        # print(showMap())
        conn.close()
    
    except ConnectionResetError:
        print(f"ERROR!! DESCONEXIÓN DEL TAXI {idTaxi} NO ESPERADA.")
        taxis.remove(idTaxi)

# Abre un socket para aceptar peticiones de autenticación
def connectionSocket(server):
    server.listen()
    print(f"CENTRAL A LA ESCUCHA EN {server}")
    while True:
        conn, addr = server.accept()
        
        threadAuthTaxi = threading.Thread(target=authTaxi, args=(conn,))
        threadAuthTaxi.start()

# Crear un productor y enviar un mensaje a través de Kafka con un topic y su mensaje
def sendMessageKafka(topic, msg):
    time.sleep(0.1)
    producer = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    producer.send(topic, msg.encode(FORMAT))
    producer.flush()
    producer.close()

# Comprobar que los taxis y customers están activos (10 segundos)
def areActive():
    try:
        while True:
            sendMessageKafka("Central2Taxi", "TAXI STATUS")
            sendMessageKafka("Central2Customer", "CUSTOMER STATUS")

            activeTaxis = []
            activeCustomers = []
            consumer = kafka.KafkaConsumer("Status", group_id="centralGroup", bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])
            
            startTime = time.time()
            while True:
                if time.time() - startTime > 10:
                    break
                
                messages = consumer.poll(1000)
                for topics, messagesValues in messages.items():
                    for msg in messagesValues:
                        id = (msg.value.decode(FORMAT)).split(" ")[1]
                        try:
                            taxiID = int(id)
                            activeTaxis.append(taxiID)

                        except Exception:
                            customerID = id
                            activeCustomers.append(customerID)

            for taxi in taxis:
                if taxi not in activeTaxis:
                    sendMessageKafka("Central2Taxi", f"FIN {taxi}")
                    print(f"DESCONEXIÓN DEL TAXI {taxi} NO ESPERADA.")
                    taxis.remove(taxi)
                    searchTaxiID(0)
                    # print(showMap())
                
            for customer in customers:
                if customer[0] not in activeCustomers:
                    sendMessageKafka("Central2Customer", f"FIN {customer[0]}")
                    print(f"DESCONEXIÓN DEL CLIENTE {customer[0]} NO ESPERADA.")
                    customers.remove(customer)
                    # print(showMap())

            consumer.close()

    except kafka.errors.NoBrokersAvailable:
        print("ERROR DE KAFKA!!")

# Leer todas las solicitudes de los clientes
def requestCustomers():
    try:
        consumer = kafka.KafkaConsumer("Customer2Central", bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])
        for msg in consumer:
            id = (msg.value.decode(FORMAT)).split(" ")[0]
            ubicacion = (msg.value.decode(FORMAT)).split(" ")[1]
            print(msg.value.decode(FORMAT))

            for customer in customers:
                if customer[0] == id:
                    customers.remove(customer)

            if setTaxiDestination(ubicacion, id):
                customers.append((id, ubicacion, "OK."))
                sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO OK.")
                sendMessageKafka("Central2Taxi", f"{ubicacion}")
            else:
                customers.append((id, ubicacion, "KO."))
                sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO KO.")

            # print(showMap())
    
    except kafka.errors.NoBrokersAvailable:
        print("ERROR DE KAFKA!!")

# Función que recibe las incidencias desde Kafka
def recibir_estado_taxi():
    consumer = kafka.KafkaConsumer('InfoEstadoTaxi', bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], group_id="centralGroup")

    for mensaje in consumer:
        mensj = mensaje.value.decode(FORMAT)
        print(f"[CENTRAL] Estado Taxi recibido: {mensj}")

# PARA MOSTRAR MAPA
###############################################################################
def hilo_mostrar_mapa():
    # while True:
        mostrar_mapa(mapa, taxis, customers)
        #time.sleep(1)  # Espera 1 segundo antes de mostrar el mapa nuevamente
###############################################################################

# Función encargada de recibir el movimiento del taxi
def recibir_movimiento_taxi():
    consumer = kafka.KafkaConsumer('TaxiMovimiento', bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"], group_id="centralGroup")

    for mensaje in consumer:
        mensj = mensaje.value.decode(FORMAT)
        print(f"[CENTRAL] Movimiento recibido: {mensj}")

        # Actualiza el mapa y muestra la nueva posición
        actualizar_mapa_con_movimiento(mensj)

# Función encargada de actualizar el mapa
def actualizar_mapa_con_movimiento(mensaje):
    global mapa
    partes = mensaje.split(", ")
    id_taxi = partes[0].split(" ")[1]
    direccion = partes[1].split(": ")[1].strip()
    print(f"YEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE {direccion}")
    posicion = partes[2].split(": ")[1].strip()
    print(posicion)
    x, y = map(int, posicion.replace("POSICIÓN: x=", "").split(", y="))

    # Limpiar la posición anterior del taxi
    for row in range(len(mapa)):
        for col in range(len(mapa[row])):
            if isinstance(mapa[row][col], int) and mapa[row][col] == int(id_taxi):
                mapa[row][col] = "X"  # Cambia la posición anterior a "X"

    # Actualiza la nueva posición en el mapa y verifica la dirección
    if 0 <= x < len(mapa) and 0 <= y < len(mapa[0]):
        if direccion == "Norte":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Sur":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Este":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Oeste":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Noreste":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Sureste":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Noroeste":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        elif direccion == "Suroeste":
            mapa[x][y] = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"
        else:
            print(f"[ERROR] Dirección no reconocida: {direccion}")

    # Muestra el mapa actualizado
    mostrar_mapa(mapa, taxis, customers)

def main(port):
    server = socket.gethostbyname(socket.gethostname())

    addr = (server, int(port))
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(addr)

    print("CENTRAL INICIÁNDOSE...")

    #map = array.array('B', bytes([0] * 400))

    searchTaxiID(0)

    threadSockets = threading.Thread(target=connectionSocket, args=(server,))
    threadSockets.start()

    threadResquests = threading.Thread(target=requestCustomers)
    threadResquests.start()

    threadResquests = threading.Thread(target=areActive)
    threadResquests.start()

    threadInfoEstadoTaxi = threading.Thread(target=recibir_estado_taxi)
    threadInfoEstadoTaxi.start()

    # PARA MOSTRAR MAPA
    #######################################################################
    threadMostrarMapa = threading.Thread(target=hilo_mostrar_mapa)
    threadMostrarMapa.start()

    threadMovimientoTaxi = threading.Thread(target=recibir_movimiento_taxi)
    threadMovimientoTaxi.start()
    ########################################################################

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        KAFKA_IP = sys.argv[2]
        KAFKA_PORT = sys.argv[3]

        main(sys.argv[1])

    else:
        print("ERROR! Se necesitan estos argumentos: <PUERTO DE ESCUCHA> <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER>")
