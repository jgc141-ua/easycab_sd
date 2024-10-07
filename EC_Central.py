import sys
import socket
import threading

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
            conn.send(f"HOLA CLIENTE: He recibido tu mensaje: {msg} ".encode(FORMAT))
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
        

def start(server):
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {server}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1

def read_DB(id_Taxi):
    saved = False
    with open("database.txt", "r") as file:
        for line in file:
            if (id_Taxi == int(line)):
                saved = True
                break

    return saved

def main(port, ip_queues, port_queues):
    server = socket.gethostbyname(socket.gethostname())
    addr = (server, int(port))
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(addr)

    print("[STARTING] Servidor inicializándose...")

    start(server)

    return 0

if __name__ == "__main__":
    if  (len(sys.argv) == 2):
        main(sys.argv[1], 0, 0)#, sys.argv[2], sys.argv[3])

    else:
        print("ERROR! Se necesitan estos argumentos: <PUERTO DE ESCUCHA> <IP DEL BOOTSTRAP-SERVER> <PUERTO DEL BOOTSTRAP-SERVER>")