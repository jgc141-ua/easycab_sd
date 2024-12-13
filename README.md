CIFRADO DEL CANAL SOCKET ENTRE EC_CENTRAL Y EC_DE 

Se realiza un sistema de cifrado asimétrico con intercambio de claves y certificados

Se ha implementado un sistema de comunicación seguro entre la central (EC_Central) y los taxis (EC_DE) usando cifrado asimétrico con SSL/TLS. 
Se han generado certificados (ca.pem, central.crt, taxi.crt) para autenticar y cifrar la comunicación. 
La central usa un socket seguro para aceptar conexiones de los taxis, y estos verifican su autenticidad antes de enviar datos. 
Esto protege contra ataques como MITM, asegurando la privacidad y la integridad de los mensajes.

Tenemos:
    CERTIFICADO RAÍZ: Encargado de firmar los otros dos certificados y verificar la autenticidad
        Clave privada: ca.key
        Certificado público: ca.pem
    CERTIFICADO DEL SERVIDOR: Certificado de la central
        Clave privada: central.key
        Solicitud de certificado: central.crt
    CERTIFICADO DEL CLIENTE: Certificado del taxi
        Clave privada: taxi.key
        Solicitud de certificado: taxi.crt

Se ha añadido import ssl a ambos módulos (EC_CENTRAL Y EC_DE)

Parte anterior de EC_Central: (SIN CIFRADO)
# Abre un socket para aceptar peticiones de autenticación
def connectionSocket(server):
    server.listen()
    print(f"CENTRAL A LA ESCUCHA EN {server}")
    while True:
        conn, addr = server.accept()
        
        threadAuthTaxi = threading.Thread(target=authTaxi, args=(conn,))
        threadAuthTaxi.start()

Parte actual de EC_Central: (CON CIFRADO)
# Abre un socket para aceptar peticiones de autenticación con SSL/TLS
def connectionSocket():
    # Crear contexto SSL para el servidor
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="central.crt", keyfile="central.key")
    context.load_verify_locations("ca.pem")

    # Crear el socket del servidor
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("127.0.0.1", 5001))
    server_socket.listen(5)
    print(f"CENTRAL A LA ESCUCHA EN 127.0.0.1:5001 (SSL/TLS habilitado)")

    while True:
        # Aceptar conexiones entrantes
        client_socket, addr = server_socket.accept()

        # Envolver el socket con SSL
        ssl_client_socket = context.wrap_socket(client_socket, server_side=True)
        print(f"CONEXIÓN SSL ESTABLECIDA CON {addr}")

        # Crear un hilo para manejar la autenticación del taxi
        threadAuthTaxi = threading.Thread(target=authTaxi, args=(ssl_client_socket,))
        threadAuthTaxi.start()

Parte anterior de EC_DE: (SIN CIFRADO)
# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets
def centralConn(centralIP, centralPort):
    if not(registerTaxi()):
        return None

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as createdSocket:
            createdSocket.connect((centralIP, int(centralPort)))
            authMessage = f"AUTENTICAR TAXI #{TAXI_ID}"
            createdSocket.send(authMessage.encode(FORMAT))

            respuesta = createdSocket.recv(HEADER).decode(FORMAT)
            print(f"{respuesta}")

            if respuesta.split("\n")[1] == "VERIFICACIÓN SUPERADA.":
                sendMessageKafka("Status", f"TAXI {TAXI_ID} ACTIVO.")
                return createdSocket
        
    except ConnectionError:
        print(f"Error de conexión con la central en {centralIP}:{centralPort}")

    except Exception as e:
        print(f"Error generado: {str(e)}")
        
    return None

Parte actual de EC_DE: (CON CIFRADO)
import ssl
import socket

# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets con SSL
def centralConn(centralIP, centralPort):
    if not registerTaxi():
        return None

    try:
        # Crear contexto SSL para el cliente
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_verify_locations("ca.pem")  # Verificar con la CA
        context.load_cert_chain(certfile="taxi.crt", keyfile="taxi.key")  # Certificado y clave del taxi

        # Crear conexión SSL
        with socket.create_connection((centralIP, int(centralPort))) as createdSocket:
            with context.wrap_socket(createdSocket, server_hostname=centralIP) as ssl_sock:
                # Enviar mensaje de autenticación
                authMessage = f"AUTENTICAR TAXI #{TAXI_ID}"
                ssl_sock.send(authMessage.encode(FORMAT))

                # Recibir respuesta de la central
                respuesta = ssl_sock.recv(HEADER).decode(FORMAT)
                print(f"{respuesta}")

                if respuesta.split("\n")[1] == "VERIFICACIÓN SUPERADA.":
                    sendMessageKafka("Status", f"TAXI {TAXI_ID} ACTIVO.")
                    return ssl_sock

    except ConnectionError:
        print(f"Error de conexión con la central en {centralIP}:{centralPort}")

    except Exception as e:
        print(f"Error generado: {str(e)}")

    return None
