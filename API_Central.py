#region LIBRARIES
from flask import Flask, Response, jsonify
import ssl
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

#region FILES
DATABASE_FILE = "database.txt"
AUDIT_LOG = "audit.log"

#region FUNCTIONS
lastLog = "" # Ejemplo
fistRead = True
def readLogs():
    global lastLog, fistRead
    lines = []
    exists = False
    with open(AUDIT_LOG, "r") as logFile:
        linesInLogFile = logFile.readlines()

        if fistRead:
            if linesInLogFile:
               lastLog = linesInLogFile[-1].strip()
            
            fistRead = False
            return linesInLogFile

        try:
            lastIndexLog = linesInLogFile.index(lastLog + '\n')
            lines = linesInLogFile[lastIndexLog + 1:]
            if lines:
                lastLog = lines[-1].strip()

        except ValueError:
            if linesInLogFile:
                lastLog = linesInLogFile[-1].strip()

    return lines

# Leer todos los taxis de la base de datos
def readTaxis():
    taxiIDs = []
    with open("database.txt", "r") as file:
        for line in file: # 4,-,OK.Parado (ejemplo)
            object = line.split(",")[0]
            if object == "TAXI":
                taxiID = int(line.split(",")[1]) # 4 (ejemplo)
                status = line.split(",")[3] # OK.Parado (ejemplo)
                active = status.split(".")[0] # OK (ejemplo)
                service = status.split(".")[1].split("\n")[0] # Parado (ejemplo)

                if active == "OK" or active == "KO":
                    taxiIDs.append((taxiID, active, service))

    return taxiIDs

# Leer todos los clientes de la base de datos
def readCustomers():
    customerIDs = []
    with open("database.txt", "r") as file:
        for line in file: # CUSTOMER,a,A,OK. Taxi 2 (ejemplo)
            object = line.split(",")[0]
            if object == "CUSTOMER":
                customerID = line.split(",")[1] # a (ejemplo)
                destination = line.split(",")[2] # A (ejemplo)
                status = line.split(",")[3] # OK. Taxi 2 (ejemplo)
                active = status.split(".")[0] # OK (ejemplo)
                service = status.split(".")[1].split("\n")[0] # Parado (ejemplo)

                if active == "OK" or active == "KO":
                    customerIDs.append((customerID, destination, active))

    return customerIDs

# Leer el mapa de toda la situación
def readMapa():
    taxiIDs = readTaxis()
    objects = []
    with open("mapa.txt", "r") as file:
        for line in file:
            object = line.split(",")[0]
            if object == "MAPA":
                id = line.split(",")[1]
                x = line.split(",")[2].split(".")[0]
                y = line.split(",")[2].split(".")[1].split("\n")[0]
                objects.append((id, x, y))

    return objects


#region URL METHODS
# Encabezados de cors
def corsHeaders(jsonResponse):
    jsonResponse.headers.add('Access-Control-Allow-Origin', '*')
    jsonResponse.headers.add('Access-Control-Allow-Methods', 'GET')
    jsonResponse.headers.add('Access-Control-Allow-Headers', 'Content-Type')

    return jsonResponse

# Devolver los logs no leídos
@app.route("/logs", methods=["GET"])
def getLogs():
    lines = readLogs()
    return corsHeaders(jsonify(lines))

# Devolver los taxis activos
@app.route("/taxis", methods=["GET"])
def getTaxis():
    taxis = readTaxis()
    return corsHeaders(jsonify(taxis))

# Devolver los clientes activos
@app.route("/customers", methods=["GET"])
def getCustomers():
    customers = readCustomers()
    return corsHeaders(jsonify(customers))

# Devolver los objetos del mapa, sus posiciones y colores
@app.route("/objects", methods=["GET"])
def getMapa():
    objects = readMapa()
    return corsHeaders(jsonify(objects))
    

#region MAIN
if __name__ == "__main__":
    try:
        sslContext = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sslContext.load_cert_chain("certAndKey.pem")

        app.run(
            host="192.168.24.1",
            port=5151,
            ssl_context=sslContext,
            threaded=True
        )
    except Exception as e:
        print(f"ERROR EN EL SERVIDOR!! {str(e)}")