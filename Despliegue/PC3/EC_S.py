# Sensors
# Aplicación que simula la funcionalidad de los sensores embarcados en el vehículo 

import socket
import time
import threading
import sys
import msvcrt

HEADER = 64
FORMAT = 'utf-8'
OK = "OK"
KO = "KO"
FIN = "FIN"
DESCONECTADO = False
INCIDENCIA_DETECTADA = False
TIPO_DE_INCIDENCIA = ""
TAXI_CAIDO = False

# Función encargada de conectar el sensor con el digital engine
def conectar_a_DE(ip_DE, puerto_DE):
    intentos = 0
    max_intentos = 5

    while intentos < max_intentos:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip_DE, int(puerto_DE)))
            print(f"[CONECTADO] Sensor conectado al Digital Engine en {ip_DE}:{puerto_DE}")
            return s
        
        except ConnectionError:
            intentos += 1
            print(f"[ERROR] No se pudo conectar al Digital Engine en {ip_DE}:{puerto_DE}. Intento {intentos}/{max_intentos}. Reintentando...")
            time.sleep(3)  

    print(f"[FALLO] No se pudo establecer conexión después de {max_intentos} intentos. Cerrando sensor.")
    return None

# Función encargada de capturar una tecla, sin tener que pulsar Enter en Windows
def capturar_tecla():
    global TAXI_CAIDO

    if TAXI_CAIDO == True:
        return None
    
    try:
        if msvcrt.kbhit():  
            return msvcrt.getch().decode(FORMAT).lower()
    
    except Exception:
        print("Tecla no asgnada a una incidencia")

    return None

# Función encargada de enviar el estado a Digital Engine
def enviar_estado(ip_DE, port_DE):
    global DESCONECTADO
    global INCIDENCIA_DETECTADA
    global TIPO_DE_INCIDENCIA
    global TAXI_CAIDO

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
            socket_creado.connect((ip_DE, int(port_DE)))
            print(f"[CONECTADO] Conectado a {ip_DE}:{port_DE}")

            while not DESCONECTADO:
                try:
                    estado = KO if INCIDENCIA_DETECTADA else OK

                    if INCIDENCIA_DETECTADA:
                        estado = estado + f", INCIDENCIA: {TIPO_DE_INCIDENCIA}"

                    socket_creado.send(estado.encode(FORMAT))

                    print(f"[ENVÍO] Estado enviado: {estado}")
                    time.sleep(1)

                except Exception as e:
                    print("Taxi caído") 
                    print("Sensor sin taxi, sensor roto, cae sensor")
                    TAXI_CAIDO = True
                    sys.exit(1)
    
    except ConnectionRefusedError as e:
        print("Taxi sin autenticar, sensor inhabilitado")
        TAXI_CAIDO = True
        sys.exit(1)

# Función encargada de detectar incidencias cuando se presiona una tecla
def detectar_incidencia():
    global DESCONECTADO
    global INCIDENCIA_DETECTADA
    global TIPO_DE_INCIDENCIA
    global TAXI_CAIDO

    while not DESCONECTADO and not TAXI_CAIDO:

        if TAXI_CAIDO == True:
            break

        tecla_capturada = capturar_tecla()

        if tecla_capturada is not None:
            # SEMÁFORO
            if tecla_capturada == 's':
                TIPO_DE_INCIDENCIA = "SEMÁFORO EN ROJO"
                INCIDENCIA_DETECTADA = True
            # PERSONA
            elif tecla_capturada == 'p':
                TIPO_DE_INCIDENCIA = "PEATÓN CRUZANDO"
                INCIDENCIA_DETECTADA = True
            # COCHE
            elif tecla_capturada == 'c':
                TIPO_DE_INCIDENCIA = "VEHÍCULO EN MEDIO"
                INCIDENCIA_DETECTADA = True
            # VALLA
            elif tecla_capturada == 'v':
                TIPO_DE_INCIDENCIA = "VALLA ENFRENTE"
                INCIDENCIA_DETECTADA = True
            # MURO
            elif tecla_capturada == 'm':
                TIPO_DE_INCIDENCIA = "MURO ENFRENTE"   
                INCIDENCIA_DETECTADA = True     
            # OBRA
            elif tecla_capturada == 'o':
                TIPO_DE_INCIDENCIA = "OBRA, CALLE CORTADA"
                INCIDENCIA_DETECTADA = True
            # STOP
            elif tecla_capturada == 't':
                TIPO_DE_INCIDENCIA = "STOP, ANIMALES CRUZANDO"
                INCIDENCIA_DETECTADA = True
            # ACCIDENTE
            elif tecla_capturada == 'a':
                TIPO_DE_INCIDENCIA = "ACCIDENTE DELANTE"
                INCIDENCIA_DETECTADA = True
            # SOLUCIÓN DE INCIDENCIA
            elif tecla_capturada == ' ':
                print("INCIDENCIA SOLUCIONADA")
                INCIDENCIA_DETECTADA = False
            else:
                print('Tecla no asgnada a una incidencia')

    if TAXI_CAIDO == True:
        sys.exit(1)
           
# Sensor
def sensor(ip_DE, port_DE):
    global TAXI_CAIDO

    conexion_digital_engine = conectar_a_DE(ip_DE, port_DE)

    if conexion_digital_engine:

        threadEnviaEstado = threading.Thread(target=enviar_estado, args=(ip_DE, port_DE))
        threadDetectaIncidencia = threading.Thread(target=detectar_incidencia)

        threadEnviaEstado.start()
        threadDetectaIncidencia.start()  

        threadEnviaEstado.join()
        threadDetectaIncidencia.join()

        if TAXI_CAIDO == True:
            sys.exit(1)

    else:
        sys.exit(1)

# Main
if __name__ == "__main__":
    if len(sys.argv) == 3:
        ip_DE = sys.argv[1]
        port_DE = int(sys.argv[2])

        sensor(ip_DE, port_DE)

    else:
        print(f"ERROR!! Falta por poner <IP EC_D> <PUERTO EC_D>")
