import sys
import kafka
import threading
import json
import time

FORMAT = 'utf-8'
KAFKA_IP = 0
KAFKA_PORT = 0

# Crear un productor y enviar un mensaje a través de Kafka con un topic y su mensaje
def sendMessageKafka(topic, msg):
    time.sleep(0.4)
    producer = kafka.KafkaProducer(bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    producer.send(topic, msg.encode(FORMAT))
    producer.flush()
    producer.close()

def statusMessages(ID):
    sendMessageKafka("Status", f"CUSTOMER {ID} ACTIVO.")

# Solicita un taxi
def requestTaxi(ID, destino):
    print(f"{ID} {destino}")
    sendMessageKafka("Customer2Central", f"{ID} {destino}")
    sendMessageKafka("Status", f"CUSTOMER {ID} ACTIVO.")

    consumer = kafka.KafkaConsumer("Central2Customer", group_id="customerGroup", bootstrap_servers=f"{KAFKA_IP}:{KAFKA_PORT}")
    for msg in consumer:
        message = msg.value.decode(FORMAT)
        print(message)
        
        if message == "CUSTOMER STATUS":
            threading.Thread(target=statusMessages, args=(ID,)).start()

        if message != "CUSTOMER STATUS" and message != f"FIN {ID}":
            estado = message.split(" ")[3]
            print(estado)

            if(estado == "KO."):
                print("SERVICIO RECHAZADO. ENVIANDO EL SIGUIENTE SERVICIO...")
                break
            elif(estado == "OK."):
                print("SERVICIO ACEPTADO.")

        elif message == f"FIN {ID}":
            consumer.close()
            return True
        
        elif message == "DEATH CENTRAL":
            consumer.close()
            print("SE HA PERDIDO LA CONEXIÓN CON LA CENTRAL.")
            return True

def main(ID):
    with open(f"Requests/EC_Requests_{ID}.json", "r", encoding="utf-8") as requests:
        destinos = json.load(requests)
    
    for destino in destinos["Requests"]:
        if requestTaxi(ID, destino["Id"]):
            break

        time.sleep(4)

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        KAFKA_IP = sys.argv[1]
        KAFKA_PORT = sys.argv[2]
        
        main(sys.argv[3])

    else:
        print("ERROR! Se necesitan estos argumentos: <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER> <ID DEL CLIENTE>")