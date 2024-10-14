import sys
import kafka
import threading

FORMAT = 'utf-8'

def requestTaxi(kafkaIP, kafkaPort, ID):
    producer = kafka.KafkaProducer(bootstrap_servers=f"{kafkaIP}:{kafkaPort}")
    producer.send("Clientes", f"{ID}".encode(FORMAT))
    producer.flush()

    consumer = kafka.KafkaConsumer("Clientes")
    for msg in consumer:
        message = msg.value.decode(FORMAT)
        print(message)
        if(message[-1] == "5"):
            break


def main(kafkaIP, kafkaPort, ID):
    threadKafka = threading.Thread(target=requestTaxi, args=(kafkaIP, kafkaPort, ID))
    threadKafka.start()

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 4):
        main(sys.argv[1], sys.argv[2], sys.argv[3])

    else:
        print("ERROR! Se necesitan estos argumentos: <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER> <ID DEL CLIENTE>")