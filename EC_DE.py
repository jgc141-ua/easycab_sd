# Digital Engine
# Aplicación que implementará la lógica principal de todo el sistema

# Librerías
import socket # Para establecer la conexión de red entre las aplicaciones del sistema
import sys # Para acceder a los argumentos de la línea de comandos
#from kafka import kafkaProducer, KafkaConsumer   # type: ignore
import json
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = 'FIN'

# Inicialización de la posición del taxi (indefinida)
posicion_actual = None

# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets
def conexion_central(ip_central, port_central, id_taxi):
    try:
        # Creación de un socket IP/TCP
            # AF_INET -> se usará IPv4
            # SOCK_STREAM -> se usará TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
            # Conexión del socket creado con el servidor (central de control) mediante la IP y PUERTO dados
            socket_creado.connect((ip_central, int(port_central)))

            # Autenticación con la central enviando el ID del taxi
            mensaje_de_autenticacion = f'AUTENTICAR TAXI #{id_taxi}'

            # Envío del mensaje de autenticación
            socket_creado.sendall(mensaje_de_autenticacion.encode(FORMAT))

            # Espera de la respuesta de la central
            respuesta = socket_creado.recv(HEADER).decode(FORMAT)

            # Mostramos la respuesta de la central
            print(f"{respuesta}")

            # Comprobamos si la autenticación fue válida o errónea
            if respuesta[len(respuesta)-1] == "1":
                return socket_creado # Devolvemos el socket para usarlo después
            else:
                return None
        
    except ConnectionError:
        print(f"Error de conexión con la central en {ip_central}:{port_central}")
    except Exception as e:
        print(f"Error generado: {str(e)}")
        return None
    
# Función encargada de enviar eventos del taxi a través de kafka
def enviar_eventos_kafka(productor, id_taxi, mensaje_kafka):
    global posicion_actual

    if posicion_actual is not None:
        evento = {
            'tipo' : 'movimiento',
            'id_taxi' : id_taxi,
            'posicion' : f'x:{posicion_actual["x"]}, y:{posicion_actual["y"]}'
        }

        # Mensaje en kafka
        productor.send(mensaje_kafka, value=evento)
        productor.flush()

        print(f"[EVENTO] Taxi {id_taxi} publicó su posición en kafka: {evento['posicion']}")
    else:
        print(f"[ERROR] Taxi no tiene una posición definida para enviar")

# Función encargada de escuchar instrucciones desde kafka
def recibir_mensajes_kafka(consumidor, id_taxi):
    print(f"[ESPERANDO] Taxi {id_taxi} experando mensajes de la central...")

    for mensaje in consumidor:
        datos = mensaje.value
        print(f"Mensaje recibido de la central: {datos}")

        if datos['tipo'] == 'instruccion':
            print(f"Instrucción recibida: {datos['instruccion']}")
            if datos['instruccion'] == 'parar':
                print(f"Taxi {id_taxi} se detiene")
            elif datos['instruccion'] == 'reanudar':
                print(f"Taxi {id_taxi} reanuda el proyecto")
            elif datos['instruccion'] == 'mover':
                posicion_nueva = datos['posicion_nueva']
                posicion_actual = {'x': posicion_nueva['x'], 'y' : posicion_nueva['y']}
                print(f"Taxi {id_taxi} movido a la nueva posición: {posicion_actual}")
            elif datos['instruccion'] == FIN:
                print(f"La central ha finalizado la conexión con el Taxi {id_taxi}")
                break

# Función principal de conexión del taxi
def conexion_taxi(ip_central, port_central, ip_broker, port_broker, ip_sensores, port_sensores, id_taxi):
    # Conexión con la central
    socket_creado = conexion_central(ip_central, port_central, id_taxi)

    if False:#socket_creado:
        # Inicializamos el productor y el consumidor de kafka
        broker = f'{ip_broker}:{port_broker}'
        productor = kafkaProducer(bootstrap_servers = [broker], value_serializer = lambda v: json.dumps(v).encode(FORMAT))

        topic_actualizacion_central = "actualizacion_central"
        topic_actualizacion_taxi = f"actualización_taxi_{id_taxi}"

        consumidor = KafkaConsumer(topic_actualizacion_taxi, 
                                   bootstrap_server = [broker],
                                   auto_offset_reset = 'earliest',
                                   value_deserializer = lambda x: json.loads(x.decode(FORMAT)))
        
        # Mientras la conexión se encuentre activa, enviamos eventos y recibimos instrucciones
        while True:
            enviar_eventos_kafka(productor, id_taxi, topic_actualizacion_central)
            recibir_mensajes_kafka(consumidor, id_taxi)
            time.sleep(1)

# Main
if __name__ == "__main__":
    # Se esperan 8 argumentos
    if len(sys.argv) == 8:
        ip_central = sys.argv[1]
        port_central = sys.argv[2]
        ip_broker = 0#sys.argv[3]
        port_broker = 0#sys.argv[4]
        ip_sensores = 0#sys.argv[5]
        port_sensores = 0#sys.argv[6]
        id_taxi = sys.argv[7]

        # Conexión del taxi con la central y kafka
        conexion_taxi(ip_central, port_central, ip_broker, port_broker, ip_sensores, port_sensores, id_taxi)

    else:
        print(f"ERROR!! Falta por poner <IP EC_CENTRAL> <PUERTO EC_CENTRAL> <IP GESTOR DE COLAS> <PUERTO GESTOR DE COLAS> <IP EC_S> <PUERTO EC_S> <ID TAXI>")

