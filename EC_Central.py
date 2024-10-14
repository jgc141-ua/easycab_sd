import sys
import socket
import threading
import kafka
import array
import time

HEADER = 64
FORMAT = 'utf-8'
END_CONNECTION1 = "FIN"
END_CONNECTION2 = "ERROR"
MAX_CONEXIONES = 100

def showMap():
    print(" ------------------------------------------------------------ ")
    print("|                          EASY CAB                          |")
    print(" -----------------------------|------------------------------ ")
    print("|            Taxis            |           Clientes           |")
    print(" -----------------------------|------------------------------ ")
    print("| Id.  Destino    Estado      | Id.  Destino     Estado      |")
    print(" -----------------------------|------------------------------ ")

    with open("database.txt", "r") as file:
        for line in file:
            id = int(line.split(",")[0].strip())
            destino = line.split(",")[1].strip()
            estado = line.split(",")[2].strip()

            activo = estado.split(".")[0].strip()
            servicio = estado.split(".")[1].strip()
            
            if servicio == "Parado":
                servicio += "   "

            if activo != "NO":
                print(f"|  {id}     {destino}      {activo}. {servicio} |", end="")
                print("  1      d      OK. Taxi 5    |")
    
    print(" -----------------------------|------------------------------ ")

def searchTaxiID(idTaxi, taxis):
    modifiedLine = []
    exists = False
    with open("database.txt", "r+") as file:
        for line in file:
            id = int(line.split(",")[0])
            if idTaxi == id and idTaxi not in taxis:
                exists = True
                taxis.append(idTaxi)
                line = line.replace("NO", "OK")
                modifiedLine.append(line)

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
    with open("database.txt", "r+") as file:
        for line in file:
            destinationTaxi = int(line.split(",")[1])
            status = int(line.split(",")[2])

            if destinationTaxi == "-" and status == "OK":
                line = line.replace("-", destination)
                line = line.replace("Parado", f"Servicio {idCustomer}")
                modifiedLine.append(line)
            
            else:
                modifiedLine.append(line)

        file.seek(0)
        file.writelines(modifiedLine)
        file.truncate()

def verifyTaxi(conn, idTaxi, taxis):
    isVerified = searchTaxiID(idTaxi, taxis)

    if isVerified:
        print(f"NUEVO TAXI {idTaxi} CONFIRMADO. VERIFICACIÓN SUPERADA.")
        conn.send("ESPERANDO VERIFICACIÓN... VERIFICACIÓN SUPERADA. /1".encode(FORMAT))

    return isVerified

def receiveClient(conn, taxis):
    idTaxi = -1
    
    try:
        print("NUEVO TAXI CONECTADO. ESPERANDO VERIFICACIÓN...")

        it = 0
        connected = True
        isVerified = True
        while connected and isVerified:
            msg = conn.recv(HEADER).decode(FORMAT)

            if msg == "FIN":
                connected = False
            
            if it == 0:
                idTaxi = int(msg.split("#")[1])
                print(idTaxi)
                isVerified = verifyTaxi(conn, idTaxi, taxis)
                showMap()

            it = 1

        if isVerified:
            print(f"DESCONECTANDO TAXI {idTaxi}...")
            taxis.remove(idTaxi)
        else:
            print(f"NUEVO TAXI {idTaxi} RECHAZADO. DESCONECTANDO...")
            conn.send("VERIFICACIÓN NO SUPERADA. DESCONECTANDO... /0".encode(FORMAT))
        
        conn.close()
    
    except ConnectionResetError:
        print(f"ERROR!! DESCONEXIÓN DEL TAXI {idTaxi} NO ESPERADA.")
        taxis.remove(idTaxi)

def connectionSocket(server, taxis):
    server.listen()
    print(f"CENTRAL A LA ESCUCHA EN {server}")
    while True:
        conn, addr = server.accept()
        
        threadTaxi = threading.Thread(target=receiveClient, args=(conn, taxis))
        threadTaxi.start()

xdd = []

def assignCustomer(ipQueues, portQueues, idCustomer):
    producer = kafka.KafkaProducer(bootstrap_servers=f"{ipQueues}:{portQueues}")
    if idCustomer not in xdd:
        producer.send("Clientes", f"SERVICIO ACEPTADO AL CLIENTE {idCustomer} /5".encode(FORMAT))
        xdd.append(idCustomer)
    else:
        producer.send("Clientes", "Debe esperar más tiempo".encode(FORMAT))

def requestCustomers(ipQueues, portQueues):
    consumer = kafka.KafkaConsumer("Clientes")
    for msg in consumer:
        idCustomer = msg.value.decode(FORMAT)
        threadAssignCustomers = threading.Thread(target=assignCustomer, args=(ipQueues, portQueues, idCustomer))
        threadAssignCustomers.start()

def main(port, ipQueues, portQueues):
    server = socket.gethostbyname("172.27.202.8")#socket.gethostname())
    addr = (server, int(port))
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(addr)

    print("CENTRAL INICIÁNDOSE...")

    taxis = []
    #map = array.array('B', bytes([0] * 400))

    threadSockets = threading.Thread(target=connectionSocket, args=(server, taxis))
    threadSockets.start()

    #threadResquests = threading.Thread(target=requestCustomers, args=(ipQueues, portQueues))
    #threadResquests.start()

    #threadShowMap = threading.Thread(target=showMap)
    #threadShowMap.start()

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        main(sys.argv[1], sys.argv[2], sys.argv[3])

    else:
        print("ERROR! Se necesitan estos argumentos: <PUERTO DE ESCUCHA> <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER>")