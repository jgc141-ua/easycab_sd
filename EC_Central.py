import sys
import socket
import threading
import kafka
import time
import colorama
from colorama import *

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

# PARA MOSTRAR MAPA
#######################################
mapa = [["x" for _ in range(20)] for _ in range(20)]
taxis = []
customers = []
localizaciones = []

colorama.init(autoreset=True)

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
            # Si el taxi es una tupla, extraer sus valores
            if isinstance(taxis[i], tuple):
                id_taxi, destino_taxi, estado_taxi = taxis[i]
                taxi_line = f"{id_taxi:<5} {destino_taxi:<10} {estado_taxi:<10}"
            else:
                taxi_line = " " * 25
        
        if i < len(clientes):
            cliente = clientes[i]
            cliente_line = f"{cliente['id']:<5} {cliente['destino']:<10} {cliente['estado']:<10}"
        else:
            cliente_line = " " * 25
        
        sys.stdout.write(f"{taxi_line} {cliente_line}\n")

    sys.stdout.write("------------------------------------------------------------\n")

    sys.stdout.write("   " + " ".join([f"{i:2}" for i in range(1, 21)]) + "\n")
    
    sys.stdout.write("------------------------------------------------------------\n")

    # Mostrar el mapa utilizando streams
    for row in range(len(mapa)):
        # Mostrar el número de fila (ajustado de 1-20)
        sys.stdout.write(f"{row + 1:2} ")
        for col in range(len(mapa[row])):
            if isinstance(mapa[row][col], int):  # Taxis
                sys.stdout.write(Fore.GREEN + f"{mapa[row][col]:2} " + Style.RESET_ALL)
            elif mapa[row][col] == 'C':  # Clientes
                sys.stdout.write(Fore.BLUE + " C  " + Style.RESET_ALL)
            else:
                sys.stdout.write(" X ")
        sys.stdout.write("\n")  

    sys.stdout.write("------------------------------------------------------------\n")
    sys.stdout.write(f"{' ':<12} Estado general del sistema: OK\n")
    sys.stdout.write("------------------------------------------------------------\n")

    sys.stdout.flush()  # Asegurar que todo se envía a la consola
###############################################################################################################

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
            if isinstance(mapa[row][col], int) and mapa[row][col] == id_taxi:
                mapa[row][col] = "X"  # Cambia la posición anterior a "X"

    # Actualiza la nueva posición en el mapa según la dirección
    if 0 <= x < len(mapa) and 0 <= y < len(mapa[0]):
        if direccion in ["Norte", "Sur", "Este", "Oeste", "Noreste", "Sureste", "Noroeste", "Suroeste"]:
            # Guarda el número del taxi en el mapa para ser coloreado en la función mostrar_mapa
            mapa[x][y] = id_taxi
        else:
            print(f"[ERROR] Dirección no reconocida: {direccion}")

    # Muestra el mapa actualizado
    mostrar_mapa(mapa, taxis, customers)

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
customers = []

# Leer todas las solicitudes de los clientes
def requestCustomers():
    consumer = kafka.KafkaConsumer("Customer2Central", group_id="customer2CentralGroup", bootstrap_servers=[f"{KAFKA_IP}:{KAFKA_PORT}"])
    for msg in consumer:
        message = msg.value.decode(FORMAT)
        print(message)

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
            customers.append((id, ubicacion, destino, "ACEPTADO."))
            sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO ACEPTADO.")
            sendMessageKafka("Central2Taxi", f"{ubicacion} {destino}")
            mostrarTaxisCustomers()
        
        elif not isConnected:
            customers.append((id, ubicacion, "RECHAZADO."))
            sendMessageKafka("Central2Customer", f"CLIENTE {id} SERVICIO RECHAZADO.")
            mostrarTaxisCustomers()

# GESTIONA EL MANTENIMIENTO DE LOS CLIENTES O LOS TAXIS
# Deshabilitar los taxis o clientes no activos
def disableNoActives(activeTaxis, activeCustomers):
    for taxi in taxis:
        if taxi not in activeTaxis:
            sendMessageKafka("Central2Taxi", f"FIN {taxi}")
            print(f"DESCONEXIÓN DEL TAXI {taxi} NO ESPERADA.")
            taxis.remove(taxi)
            _, customer2Disconnect = manageTaxi(CLEAN) # Limpiar taxis no activos
            sendMessageKafka("Central2Customer", f"CLIENTE {customer2Disconnect} SERVICIO RECHAZADO.")
            mostrarTaxisCustomers()
        
    for customer in customers:
        if customer[0] not in activeCustomers:
            sendMessageKafka("Central2Customer", f"FIN {customer[0]}")
            print(f"DESCONEXIÓN DEL CLIENTE {customer[0]} NO ESPERADA.")
            manageTaxi(DISCONNECT, customer[0]) # Desconectar cliente de taxi
            customers.remove(customer)
            mostrarTaxisCustomers()

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

# MAIN
def main(port):
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
    threadMostrarMapa = threading.Thread(target=hilo_mostrar_mapa)
    threadMostrarMapa.start()

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
