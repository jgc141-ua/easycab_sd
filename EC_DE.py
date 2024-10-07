# Digital Engine
# Aplicación que implementará la lógica principal de todo el sistema

# Librerías
import socket # Para establecer la conexión de red entre las aplicaciones del sistema
import time # Para simular un comportamiento en tiempo real (pausas controladas en la ejecución)
import sys # Para acceder a los argumentos de la línea de comandos

# Función encargada de manejar la conexión con la central y esperar órdenes
def conexion_central(ip_central, port_central, id_taxi):

    # Creación de un socket IP/TCP
        # AF_INET -> se usará IPv4
        # SOCK_STREAM -> se usará TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_creado:
        # Conexión del socket creado con el servidor (central de control) mediante la IP y PUERTO dados
        socket_creado.connect((ip_central, port_central))

        # Autenticación con la central enviando el ID del taxi
        mensaje_de_autenticacion = f'AUTENTICAR TAXI = #{id_taxi}'

        # Envío del mensaje de autenticación
        socket_creado.sendall(mensaje_de_autenticacion.encode())

        # Espera de la respuesta de la central
        respuesta = socket_creado.recv(1024).decode()

        # Mostramos la respuesta de la central
        print(f"Respuesta de la central: {respuesta}")

        # Comprobamos si la autenticación fue válida o errónea
        if respuesta == "OK":
            print(f"Taxi {id_taxi} autenticado correctamente")
        else:
            print(f"Fallo de autenticación del taxi {id_taxi}")
            return # Terminamos la conexión
