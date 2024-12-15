import requests
from flask import Flask, jsonify, request

API_KEY = "443334ba9843827f2569d98f613957b4"  
ciudad = "Alicante"

# Función para cambiar de ciudad
def cambiar_ciudad(nueva_ciudad):
    global ciudad
    ciudad = nueva_ciudad
    print(f"Ciudad cambiada a: {ciudad}")

# Función para obtener la temperatura de la ciudad actual
def obtener_temperatura():
    global ciudad, API_KEY
    url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
    try:
        respuesta = requests.get(url)
        if respuesta.status_code == 200:
            datos = respuesta.json()
            temperatura = datos['main']['temp']
            return temperatura
        elif respuesta.status_code == 404:
            print("Ciudad no encontrada. Verifica el nombre.")
            return None
        elif respuesta.status_code == 401:
            print("Error de autenticación. Verifica tu clave API.")
            return None
        else:
            print(f"Error al obtener datos del clima. Código: {respuesta.status_code}")
            return None
    except Exception as e:
        print(f"Error al conectarse a la API: {e}")
        return None

# Función para verificar el estado de circulación
def verificar_circulacion():
    temperatura = obtener_temperatura()
    estado = None
    if temperatura is not None:
        if temperatura >= 0:
            estado = "OK"
        else:
            estado = "KO"
    return temperatura, estado

# Menú interactivo
def mostrar_menu():
    while True:
        print("\n--- Control de Tráfico Urbano ---")
        print(f"Ciudad actual: {ciudad}")
        print("1. Verificar estado de circulación")
        print("2. Cambiar ciudad")
        print("3. Salir")
        opcion = input("Seleccione una opción: ")

        if opcion == "1":
            temperatura, estado = verificar_circulacion()
            print(f"Temperatura actual: {temperatura}°C")
            print(f"Estado de circulación en {ciudad}: {estado}")
        elif opcion == "2":
            nueva_ciudad = input("Ingrese el nombre de la nueva ciudad: ")
            cambiar_ciudad(nueva_ciudad)
        elif opcion == "3":
            print("Saliendo...")
            break
        else:
            print("Opción no válida. Intente de nuevo.")

# Configuración de API REST con Flask
app = Flask(__name__)

from flask import Flask, jsonify, request, json

@app.route("/estado_circulacion", methods=["GET"])
def api_estado_circulacion():
    global ciudad
    temperatura, estado = verificar_circulacion()
    print(f"Temperatura actual: {temperatura}°C")
    print(f"Estado de circulación en {ciudad}: {estado}")    
    if temperatura is not None:
        return app.response_class(
            response=json.dumps({
                "ciudad": ciudad,
                "estado": estado,
                "temperatura": f"{temperatura} °C"
            }, ensure_ascii=False),  
            status=200,
            mimetype="application/json"
        )
    else:
        return app.response_class(
            response=json.dumps({
                "ciudad": ciudad,
                "estado": estado,
                "temperatura": "No disponible"
            }, ensure_ascii=False),  
            status=200,
            mimetype="application/json"
        )

@app.route("/cambiar_ciudad", methods=["POST"])
def api_cambiar_ciudad():
    global ciudad
    datos = request.json  
    if "ciudad" in datos:
        nueva_ciudad = datos["ciudad"]
        cambiar_ciudad(nueva_ciudad)
        return jsonify({"mensaje": f"Ciudad cambiada a {nueva_ciudad}", "ciudad": ciudad})
    else:
        return jsonify({"error": "El parámetro 'ciudad' es requerido"}), 400

# Main
if __name__ == "__main__":
    print("Inicio del sistema City Traffic Control")
    print(f"Ciudad predeterminada: {ciudad}")
    
    # Se puede elegir entre menú interactivo o API REST
    modo = input("¿Menú interactivo (1) o servidor REST (2)? ")
    
    if modo == "1":
        mostrar_menu()
    elif modo == "2":
        print("Para observar el estado de circulación, accede a http://127.0.0.1:5000/estado_circulacion")
        print("Para cambiar de ciudad, usa una solicitud POST a http://127.0.0.1:5000/cambiar_ciudad")
        app.run(port=5000)
    else:
        print("Opción no válida. Saliendo...")
