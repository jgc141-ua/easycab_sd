import sys
import kafka
import threading
import json
import time

FORMAT = 'utf-8'
KAFKA_IP = 0
KAFKA_PORT = 0
PRODUCER = 0
disconnect = False

def deleteNoNecessaryMessages(ID):
    consumer = kafka.KafkaConsumer("Central2Customer", group_id=f"servicesCustomer_{ID}", auto_offset_reset="latest", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

    startTime = time.time()
    while not disconnect:
        endTime = time.time()
        if endTime - startTime > 2:
            break
        
        messages = consumer.poll(10)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                x = 0
        
    consumer.close()

    consumer = kafka.KafkaConsumer("Central2Customer", group_id=f"statusCustomer_{ID}", auto_offset_reset="latest", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    startTime = time.time()
    while not disconnect:
        endTime = time.time()
        if endTime - startTime > 2:
            break
        
        messages = consumer.poll(10)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                x = 0
        
    consumer.close()
    
    return True


# Envía un mensaje a través de KAFKA
def sendMessageKafka(topic, msg):
    time.sleep(0.5)
    PRODUCER.send(topic, msg.encode(FORMAT))
    PRODUCER.flush()

# Obtener los servicios del cliente
def obtainServices(ID):
    try:
        ubication = 0
        services = []
        with open(f"Requests/EC_Requests_{ID}.json", "r", encoding="utf-8") as requests:
            destinos = json.load(requests)
            
            for destino in destinos["Ubication"]:
                ubication = destino["Id"]

            for destino in destinos["Requests"]:
                services.append(destino["Id"])

        return ubication, services
    
    except Exception:
        print(F"NO EXISTE EL ARCHIVO DE SERVICIOS: Requests/EC_Requests_{ID}.json")

# Ejecuta todos los servicios del cliente
def executeServices(ID):
    global disconnect
    ubication, services = obtainServices(ID)

    for service in services:
        if disconnect == True:
            print("ERROR!! NO HAY CONEXIÓN CON LA CENTRAL.")
            break

        print(f"\nENVIANDO SIGUIENTE SERVICIO ({service})...")
        completedService = requestService(ID, ubication, service)
        if completedService:
            ubication = service

        time.sleep(4)

    disconnect = True

# Solicitudes de servicio y mantenimiento de la conexión con central (CUSTOMER STATUS)
def requestService(ID, ubicacion, destino):
    global disconnect
    sendMessageKafka("Customer2Central", f"{ID} {ubicacion} {destino}") # Un taxi primero tiene que ir a la ubicación y luego llevarlo al destino

    consumer = kafka.KafkaConsumer("Central2Customer", group_id=f"servicesCustomer_{ID}", auto_offset_reset="latest", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

    completedService = False
    declinedService = False
    while not disconnect and not declinedService:
        messages = consumer.poll(1000)
        for _, messagesValues in messages.items():
            for msg in messagesValues:
                msg = msg.value.decode(FORMAT)
        
                if msg == "CUSTOMER STATUS":
                    sendMessageKafka("Status", f"CUSTOMER {ID} ACTIVO.")

                elif msg.startswith("CLIENTE"):
                    id2Verify = msg.split(" ")[1]
                    state = msg.split(" ")[3]
                    
                    if id2Verify == ID:
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
        
    consumer.close()
    return completedService

# CUSTOMER AND CENTRAL STATUS
# Envía cada 4 segundos un mensaje a la central para comprobar su actividad
def sendCentralActive():
    while not disconnect:
        sendMessageKafka("Customer2Central", f"ACTIVE?")
        time.sleep(2)

# Recibe y envía STATUS tanto del CUSTOMER como la CENTRAL
def statusControl(ID):
    global disconnect
    sendMessageKafka("Status", f"CUSTOMER {ID} ACTIVO.")
    consumer = kafka.KafkaConsumer("Central2Customer", group_id=f"statusCustomer_{ID}", auto_offset_reset="latest", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

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


# MAIN
def main(ID):
    threadExecuteServices = threading.Thread(target=executeServices, args=(ID,))
    threadStatusControl = threading.Thread(target=statusControl, args=(ID,))
    threadCentralActive = threading.Thread(target=sendCentralActive)

    print(f"INICIANDO CLIENTE {ID}...")
    if deleteNoNecessaryMessages(ID):
        threadExecuteServices.start()
        threadStatusControl.start()
        threadCentralActive.start()

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        KAFKA_IP = sys.argv[1]
        KAFKA_PORT = int(sys.argv[2])
        PRODUCER = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")

        main(sys.argv[3])

    else:
        print("ERROR! Se necesitan estos argumentos: <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER> <ID DEL CLIENTE>")