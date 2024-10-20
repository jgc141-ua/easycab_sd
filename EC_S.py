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

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
        socket_creado.connect(ip_DE, int(port_DE))
        print(f"[CONCECTADO] Conectado a {ip_DE, int(port_DE)}")

        while not DESCONECTADO:
            estado = OK
            socket_creado.send(estado.encode(FORMAT)) # Envio de estado OK a Digital Engine

            print(f"[ESTADO] Estado: {estado}")
            time.sleep(1)

# Función encargada de enviar la incidencia que se ha detectado a Digital Engine
def envio_incidencia(ip_DE, port_DE, incidencia):    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
        socket_creado.connect(ip_DE, int(port_DE))

        socket_creado.send(incidencia.encode(FORMAT))

        print(f"[INCIDENCIA] Incidencia: {incidencia}")

# Función encargada de detectar incidencias cuando se presiona una tecla
def detectar_incidencia(ip_DE, port_DE):
    global DESCONECTADO

    while not DESCONECTADO:
        # SEMÁFORO
        if keyboard.is_pressed('s'):
           envio_incidencia(ip_DE, port_DE, 'SEMÁFORO')
           time.sleep(1)
        # PERSONA
        elif keyboard.is_pressed('p'):
            envio_incidencia(ip_DE, port_DE, 'PERSONA')
            time.sleep(1)
        # COCHE
        elif keyboard.is_pressed('c'):
            envio_incidencia(ip_DE, port_DE, 'COCHE')
            time.sleep(1)
        # VALLA
        elif keyboard.is_pressed('v'):
            envio_incidencia(ip_DE, port_DE, 'VALLA')
            time.sleep(1)
        # MURO
        elif keyboard.is_pressed('m'):
            envio_incidencia(ip_DE, port_DE, 'MURO')     
            time.sleep(1)      
        # OBRA
        elif keyboard.is_pressed('o'):
            envio_incidencia(ip_DE, port_DE, 'OBRA')
            time.sleep(1)
        # STOP
        elif keyboard.is_pressed('s'):
            envio_incidencia(ip_DE, port_DE, 'STOP')
            time.sleep(1)
        # ACCIDENTE
        elif keyboard.is_pressed('a'):
            envio_incidencia(ip_DE, port_DE, 'ACCIDENTE')
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

