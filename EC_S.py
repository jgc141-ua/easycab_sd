# Sensors
# Aplicación que simula la funcionalidad de los sensores embarcados en el vehículo 

import socket
import time
import threading
import sys
import keyboard # type: ignore

HEADER = 64
FORMAT = 'utf-8'
OK = "OK"
KO = "KO"
FIN = "FIN"
DESCONECTADO = False

# Función encargada de enviar el estado a Digital Engine
def enviar_estado(ip_DE, port_DE):
    global DESCONECTADO
    addr = (ip_DE, port_DE)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
        socket_creado(addr)
        print(f"Conectado a {addr}")

        while not DESCONECTADO:
            estado = OK
            socket_creado.send(estado.encode(FORMAT))

            print(f"Estado: {estado}")
            time.sleep(1)

# Función encargada de detectar incidencias cuando se presiona una tecla
def detectar_incidencia(ip_DE, port_DE):
    global DESCONECTADO
    addr = (ip_DE, port_DE)

    while not DESCONECTADO:
        if keyboard.read_event():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
                socket_creado.connect(addr)
                socket_creado.send(KO.encode(FORMAT))
                estado = KO
                print(f"Estado: {estado}")
                time.sleep(1)

# Sensor
def sensor(ip_DE, port_DE):
    hilo_1 = threading.Thread(target=enviar_estado, args=(ip_DE, port_DE))
    hilo_2 = threading.Thread(target=detectar_incidencia, args=(ip_DE, port_DE))

    hilo_1.start()
    hilo_2.start()

    hilo_1.join()
    hilo_2.join()

# Main
if __name__ == "__main__":
    if len(sys.argv) == 3:
        ip_DE = sys.argv[1]
        port_DE = int(sys.argv[2])

        sensor(ip_DE, port_DE)

    else:
        print("Número de argumentos incorrecto")