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
def connectionSocket(server):
    # Crear contexto SSL para el servidor
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="central.crt", keyfile="central.key")
    context.load_verify_locations("ca.pem")

    # Configurar el socket ya creado para que use SSL
    server.listen(5)
    print(f"CENTRAL A LA ESCUCHA EN {server.getsockname()} (SSL/TLS habilitado)")

    while True:
        # Aceptar conexiones entrantes
        client_socket, addr = server.accept()

        # Envolver el socket con SSL
        try:
            ssl_client_socket = context.wrap_socket(client_socket, server_side=True)
            print(f"CONEXIÓN SSL ESTABLECIDA CON {addr}")

            # Crear un hilo para manejar la autenticación del taxi
            threadAuthTaxi = threading.Thread(target=authTaxi, args=(ssl_client_socket,))
            threadAuthTaxi.start()
        except ssl.SSLError as e:
            print(f"Error SSL: {e}")
            client_socket.close()

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
# Función encargada de manejar la conexión con la central y esperar órdenes, usando sockets con SSL
def centralConnSSL(centralIP, centralPort):
    print("Iniciando conexión con la central...")
    if not registerTaxi():
        print("Error: No se pudo registrar el taxi.")
        return None
    try:
        # Crear contexto SSL para el cliente
        print("Configurando contexto SSL...")
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_verify_locations("ca.pem")  # Archivo CA válido
        context.load_cert_chain(certfile="taxi.crt", keyfile="taxi.key")  # Certificado y clave del taxi

        # Opcional: Desactivar la verificación de nombres de host (para direcciones IP)
        context.check_hostname = False

        # Crear conexión TCP con la central
        print(f"Intentando conectar a la central en {centralIP}:{centralPort}...")
        with socket.create_connection((centralIP, int(centralPort))) as createdSocket:
            print("Conexión TCP establecida. Envolviendo conexión con SSL...")

            # Envolver conexión con SSL, similar a `openssl s_client`
            with context.wrap_socket(createdSocket, server_hostname=None) as ssl_sock:
                print("Conexión SSL establecida con la central.")

                # Enviar mensaje de autenticación
                authMessage = f"AUTENTICAR TAXI #{TAXI_ID}"
                print(f"Enviando mensaje de autenticación: {authMessage}")
                ssl_sock.send(authMessage.encode(FORMAT))

                # Recibir respuesta de la central
                print("Esperando respuesta de la central...")
                respuesta = ssl_sock.recv(HEADER).decode(FORMAT)
                print(f"Respuesta recibida: {respuesta}")

                if "VERIFICACIÓN SUPERADA" in respuesta:
                    print("Verificación del taxi completada con éxito.")
                    sendMessageKafka("Status", f"TAXI {TAXI_ID} ACTIVO.")
                    return ssl_sock

    except ssl.SSLError as e:
        print(f"Error SSL: {e}")

    except ConnectionError:
        print(f"Error de conexión con la central en {centralIP}:{centralPort}")

    except Exception as e:
        print(f"Error general: {e}")

    finally:
        print("Conexión cerrada.")
