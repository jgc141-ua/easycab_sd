import sys
import socket
import threading
import kafka
import signal
import time

HEADER = 64
FORMAT = 'utf-8'
END_CONNECTION1 = "FIN"
END_CONNECTION2 = "ERROR"
KAFKA_IP = 0
KAFKA_PORT = 0
taxis = []
customers = []

def centralDown(sig, frame):
    sendMessageKafka("Central2Taxi", "DEATH CENTRAL")
    sendMessageKafka("Central2Customer", "DEATH CENTRAL")
    sys.exit(0)

signal.signal(signal.SIGINT, centralDown)
signal.signal(signal.SIGTERM, centralDown)

# Mostrar el mapa
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

def mapLogic(activeTaxis):
    str2Print = ""

    for i in range(max(len(activeTaxis), len(customers))):
        if i < len(activeTaxis):
            taxi = activeTaxis[i]

            str2Print += f"|  {taxi[0]}      {taxi[1]}     {taxi[2]}. "
            if i < len(customers):
                if taxi[3] == "Parado":
                    str2Print += f"{taxi[3]}    |"
                else:
                    str2Print += f"{taxi[3]} |"

                for j in range(len(activeTaxis)):
                    taxi = activeTaxis[j]
                    customer = customers[i]

                    str2Print += f"  {customer[0]}      {customer[1]}      " # Inicio del cliente
                    if taxi[1] == customer[0]: # Destino Taxi == ID Customer
                        if taxi[2] == "OK":
                            str2Print += f"{customer[2]} Taxi {taxi[0]}    |\n"
                        elif taxi[2] == "KO":
                            str2Print += f"{customer[2]} Sin Taxi  |\n"

                    else:
                        str2Print += f"    {customer[2]}       |\n"

            elif i == len(customers):
                str2Print += " ------------------------\n"

            else:
                if taxi[3] == "Parado":
                    str2Print += f"{taxi[3]}    |\n"
                else:
                    str2Print += f"{taxi[3]} |\n"

        elif i < len(customers):
            customer = customers[i]
            str2Print += f"                              |  {customer[0]}      {customer[1]}          {customer[2]}       |\n"
            str2Print += " ------------------------------\n"

    return str2Print

def logica(activeTaxis):
    sizeTaxis = len(activeTaxis)
    sizeCustomers = len(customers)
    line = " "
    space = " "

    i = 0
    j = 0
    while i < sizeTaxis or j < sizeCustomers:
        if i < sizeTaxis:
            taxi = activeTaxis[i]
            line += f"| {taxi[0]}{space * 5} {taxi[1]}{space * 8} {taxi[2]}{space * 8}"
        
        else:
            line += "| " + (space * 29)

        if j < sizeCustomers:
            customer = customers[j]
            line += f"| {customer[0]}{space * 5} {customer[1]}{space * 8} {customer[2]}{space * 8}"
            j += 1

            if i < sizeTaxis and "Parado" in taxi[2]:
                while j < sizeCustomers:
                    customer = customers[j]
                    line += "| " + space * 29 + f"| {customer[0]}{space * 5} {customer[1]}{space * 8} {customer[2]}{space * 8}"
                    j += 1
            
            i += 1
            while i < sizeTaxis and taxis[i][1] == customer[0]:
                line += f"| {taxis[i][0]}{space * 5} {taxis[i][1]}{space * 8} {taxis[i][2]}{space * 8}"
                i += 1

        else:
            line += "|" + (space * 29) + "|\n"
            i += 1

        line += "-" * 60 + "\n"

    return line

def logica2(activeTaxis):
    sizeTaxis = len(activeTaxis)
    sizeCustomers = len(customers)
    space = " "

    line = ""
    maxSize = max(sizeTaxis, sizeCustomers)
    for i in range(maxSize):
        if i < sizeTaxis:
            taxi = activeTaxis[i]
            line += f"| {taxi[0]}{space * 5} {taxi[1]}{space * 8} {taxi[2]}{space * 8}"
        
        else:
            line += "| " + (space * 29)

        if i < sizeCustomers:
            customer = customers[i]
            line += f"| {customer[0]}{space * 5} {customer[1]}{space * 8} {customer[2]}{space * 8}"
        
        else:
            line += f"|{space * 29}|\n"

    line += "-" * 60 + "\n"

    return line

# Mostrar un mapa
def showMap():
    activeTaxis = getTaxis()
    str2Print = ""

    str2Print += " " + "-" * 60 + " \n"
    str2Print += "|                          EASY CAB                          |\n"
    str2Print += " " + "-" * 60 + " \n"
    str2Print += "|            Taxis            |           Clientes           |\n"
    str2Print += " " + "-" * 60 + " \n"
    str2Print += "| Id.  Destino    Estado      | Id.  Destino     Estado      |\n"

    str2Print += logica2(activeTaxis)
    str2Print += "\n"

    return str2Print

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

        print(msg2Send)
        conn.send(msg2Send.encode(FORMAT))
        
        print(showMap())
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
                print(showMap())
            
        for customer in customers:
            if customer[0] not in activeCustomers:
                sendMessageKafka("Central2Customer", f"FIN {customer[0]}")
                print(f"DESCONEXIÓN DEL CLIENTE {customer[0]} NO ESPERADA.")
                customers.remove(customer)
                print(showMap())

        consumer.close()

# Leer todas las solicitudes de los clientes
def requestCustomers():
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

        print(showMap())

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

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        KAFKA_IP = sys.argv[2]
        KAFKA_PORT = sys.argv[3]

        main(sys.argv[1])

    else:
        print("ERROR! Se necesitan estos argumentos: <PUERTO DE ESCUCHA> <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER>")