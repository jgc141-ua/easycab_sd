#region LIBRARIES
import sys
import kafka
import threading
import json
import time
import uuid

#region CONSTANTS & VARIABLES
FORMAT = 'utf-8'
KAFKA_IP = 0
KAFKA_PORT = 0
PRODUCER = 0
CUSTOMER_ID = 0
disconnect = False

#region KAFKA
# Envía un mensaje a través de KAFKA
def sendMessageKafka(topic, msg):
    time.sleep(0.5)
    PRODUCER.send(topic, msg.encode(FORMAT))
    PRODUCER.flush()

#region CUSTOMER SERVICES
# Obtener los servicios del cliente
def obtainServices():
    try:
        ubication = 0
        services = []
        with open(f"Requests/EC_Requests_{CUSTOMER_ID}.json", "r", encoding="utf-8") as requests:
            destinos = json.load(requests)
            
            for destino in destinos["Ubication"]:
                ubication = destino["Id"]

            for destino in destinos["Requests"]:
                services.append(destino["Id"])

        return ubication, services
    
    except Exception:
        print(F"NO EXISTE EL ARCHIVO DE SERVICIOS: Requests/EC_Requests_{CUSTOMER_ID}.json")

    return -1, -1

# Ejecuta todos los servicios del cliente
def executeServices():
    global disconnect
    ubication, services = obtainServices()

    if ubication != -1 and services != -1:
        for service in services:
            if disconnect == True:
                print("ERROR!! NO HAY CONEXIÓN CON LA CENTRAL.")
                break

            print(f"\nENVIANDO SIGUIENTE SERVICIO ({service})...")
            completedService = requestService(ubication, service)
            if completedService:
                ubication = service

            time.sleep(4)

    disconnect = True

# Solicitudes de servicio y mantenimiento de la conexión con central (CUSTOMER STATUS)
def requestService(ubicacion, destino):
    global disconnect
    sendMessageKafka("Customer2Central", f"SOLICITUD DE SERVICIO: {CUSTOMER_ID} {ubicacion} {destino}") # Un taxi primero tiene que ir a la ubicación y luego llevarlo al destino

    consumer = kafka.KafkaConsumer("Central2Customer", group_id=str(uuid.uuid4()), auto_offset_reset="latest", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

    completedService = False
    declinedService = False
    while not disconnect and not declinedService and not completedService:
        messages = consumer.poll(1000)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                msg = msg.value.decode(FORMAT)
        
                if msg.startswith("CLIENTE"):
                    id2Verify = msg.split(" ")[1]
                    state = msg.split(" ")[3]
                    
                    if id2Verify == CUSTOMER_ID:
                        if state == "ACEPTADO.":
                            print("SERVICIO ACEPTADO Y EN CAMINO...")

                        elif state == "COMPLETADO.":
                            print("SERVICIO COMPLETADO.")
                            completedService = True
                            break

                        else:
                            print("SERVICIO RECHAZADO.")
                            declinedService = True
                            break

                elif msg.startswith(f"FIN {CUSTOMER_ID}"):
                    disconnect = True
                    break
        
    consumer.close()
    return completedService

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
# Envía cada 2 segundos un mensaje a la central para mantener el cliente activo
def customerStatus():
    while not disconnect:
        sendMessageKafka("Status", f"CUSTOMER {CUSTOMER_ID} ACTIVO.")
        time.sleep(1)

# Recibe el STATUS de la CENTRAL
def centralStatus():
    global disconnect
    consumer = kafka.KafkaConsumer("Central2Customer", group_id=str(uuid.uuid4()), bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

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
def main():
    threadExecuteServices = threading.Thread(target=executeServices)
    threadCentralStatus = threading.Thread(target=centralStatus)
    threadCustomerStatus = threading.Thread(target=customerStatus)
    threadMap = threading.Thread(target=showMap)
    threads = [threadExecuteServices, threadCentralStatus, threadCustomerStatus, threadMap]

    print(f"INICIANDO CLIENTE {CUSTOMER_ID}...")
    for thread in threads:
        thread.start()

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        KAFKA_IP = sys.argv[1]
        KAFKA_PORT = int(sys.argv[2])
        PRODUCER = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
        CUSTOMER_ID = sys.argv[3] 

        main()

    else:
        print("ERROR! Se necesitan estos argumentos: <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER> <ID DEL CLIENTE>")