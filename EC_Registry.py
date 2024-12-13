#region LIBRARIES
from flask import Flask, request, jsonify
import ssl

app = Flask(__name__)

#region DATABASE FILE
# Archivo de base de datos
DATABASE_FILE = "database.txt"

#region MESSAGES
# Mensajes tanto de error como de comando ejecutado
ID_ERROR = 0
NOTFOUND_ERROR = 1
SERVER_ERROR = 2
EXISTS_ERROR = 3
UNREGISTERED = 4
REGISTERED = 5
def messages(messageID, e=None):
    if messageID == ID_ERROR:
        return jsonify({"message": "ERROR!! FALTA LA ID DEL TAXI"}), 400
    elif messageID == NOTFOUND_ERROR:
        return jsonify({"message": "ERROR!! TAXI NO ENCONTRADO"}), 404
    elif messageID == SERVER_ERROR:
        return jsonify({"message": f"ERROR EN EL SERVIDOR: {str(e)}"}), 500
    elif messageID == EXISTS_ERROR:
        return jsonify({"message": "ERROR!! ESTE TAXI YA EXISTE O NO SE PUDO ELIMINAR"}), 409
    elif messageID == UNREGISTERED:
        return jsonify({"message": "TAXI ELIMINADO CORRECTAMENTE"}), 200
    elif messageID == REGISTERED:
        return jsonify({"message": "TAXI REGISTRADO CORRECTAMENTE"}), 200

#region (UN)REGISTER CONTROL
# Eliminar un taxi de la base de datos
def unregisterTaxi(taxiID):
    with open("database.txt", "r") as file:
        lines = file.readlines()
        file.close()

    exists = False
    with open("database.txt", "w") as file:
        for line in lines:
            id = int(line.split(",")[0])
            if id != taxiID:
                file.write(line)
            else:
                exists = True

        file.close()

    return exists

# Registrar un taxi en la base de datos
def registerTaxi(taxiID):
    with open("database.txt", "r+") as file:
        found = file.read().find(f"{taxiID}")
        if found == -1:
            file.seek(0, 2)  # Escribe al final del archivo
            file.write(f"{taxiID},-,NO.Parado\n")
            file.close()
            return True

    return False

#region  URL METHODS
# Eliminar un taxi a través de la url
@app.route("/unregister", methods=["DELETE"])
def unregisterTaxis():
    try:
        data = request.get_json()
        if not data or "id" not in data:
            return messages(ID_ERROR)
        
        taxi2UnRegister = data["id"]
        if unregisterTaxi(taxi2UnRegister):
            return messages(UNREGISTERED)
        else:
            return messages(NOTFOUND_ERROR)
            
    except Exception as e:
        return messages(SERVER_ERROR)

# Registrar un taxi a través de la url
@app.route("/register", methods=["POST"])
def registerTaxis():
    try:
        data = request.get_json()
        if not data or "id" not in data:
            return messages(ID_ERROR)
        
        taxi2Register = data["id"]
        if registerTaxi(taxi2Register):
            return messages(REGISTERED)
        else:
            return messages(EXISTS_ERROR)
            
    except Exception as e:
        return messages(SERVER_ERROR)

#region MAIN
if __name__ == "__main__":
    try:
        sslContext = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sslContext.load_cert_chain("certAndKey.pem")

        app.run(
            ssl_context=sslContext
        )
    except Exception as e:
        print(f"ERROR EN EL SERVIDOR!! {str(e)}")