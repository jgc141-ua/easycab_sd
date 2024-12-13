// CITY TRAFFIC CONTROL 

const express = require("express");
const bodyParser = require("body-parser");
const axios = require("axios");
const readline = require("readline");

const app = express();
app.use(bodyParser.json()); 

const API_KEY = "443334ba9843827f2569d98f613957b4"; 
let ciudad = "Alicante"; 

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// Función para obtener la temperatura de la ciudad actual
const obtenerTemperatura = async () => {
  const url = `http://api.openweathermap.org/data/2.5/weather?q=${ciudad}&appid=${API_KEY}&units=metric`;
  try {
    const respuesta = await axios.get(url);
    if (respuesta.status === 200) {
      return respuesta.data.main.temp;
    } else {
      console.log(`Error al obtener temperatura: Código ${respuesta.status}`);
      return null;
    }
  } catch (error) {
    console.log("Error al conectar con OpenWeather:", error.message);
    throw new Error("Error al conectar con el servicio de OpenWeather.");
  }
};

// Función para verificar el estado de circulación
const verificarCirculacion = async () => {
  const temperatura = await obtenerTemperatura();
  if (temperatura !== null) {
    return {
      temperatura,
      estado: temperatura >= 0 ? "OK" : "KO",
    };
  }
  return { temperatura: null, estado: "Error al obtener la temperatura" };
};

// Función para mostrar el menú interactivo
const mostrarMenu = () => {
  console.log("\n--- Menú Interactivo City Traffic Control ---");
  console.log(`Ciudad actual: ${ciudad}`);
  console.log("1. Verificar estado de circulación");
  console.log("2. Cambiar ciudad");
  console.log("3. Salir");

  rl.question("Seleccione una opción: ", async (opcion) => {
    switch (opcion) {
      case "1":
        try {
          const { temperatura, estado } = await verificarCirculacion();
          console.log(
            `\nTemperatura actual en ${ciudad}: ${temperatura}°C\nEstado de circulación en ${ciudad}: ${estado}`
          );
        } catch (error) {
          console.log("Error al verificar el estado de circulación:", error.message);
        }
        mostrarMenu(); 
        break;
      case "2":
        rl.question("\nIngrese el nombre de la nueva ciudad: ", (nuevaCiudad) => {
          ciudad = nuevaCiudad;
          console.log(`Ciudad cambiada a: ${ciudad}`);
          mostrarMenu(); 
        });
        break;
      case "3":
        console.log("\nSaliendo del menú interactivo...\n");
        rl.close();
        process.exit(0); 
        break;
      default:
        console.log("\nOpción no válida. Intente de nuevo.\n");
        mostrarMenu(); 
    }
  });
};

// Función para emular el comportamiento del menú desde REST
const procesarAccionRestComoMenu = async (opcion) => {
  switch (opcion) {
    case "1":
      const { temperatura, estado } = await verificarCirculacion();
      console.log(
        `\n\n[REST] Estado de circulación solicitado desde REST:\nTemperatura actual en ${ciudad}: ${temperatura}°C\nEstado de circulación en ${ciudad}: ${estado}`
      );
      break;
    case "2":
      console.log(`\n[REST] Ciudad cambiada desde REST:\nCiudad cambiada a: ${ciudad}`);
      break;
    default:
      console.log("\nAcción no válida desde REST.\n");
  }
  mostrarMenu(); 
};

// Endpoint para obtener el estado de circulación
app.get("/estado_circulacion", async (req, res, next) => {
  try {
    const { temperatura, estado } = await verificarCirculacion();
    await procesarAccionRestComoMenu("1"); 
    res.json({
      ciudad: ciudad,
      estado: estado,
      temperatura: temperatura !== null ? `${temperatura}°C` : "No disponible",
    });
  } catch (error) {
    next(error); 
  }
});

// Endpoint para cambiar la ciudad
app.post("/cambiar_ciudad", (req, res, next) => {
  try {
    const { nuevaCiudad } = req.body;
    if (!nuevaCiudad) {
      return res.status(400).json({ error: "El parámetro 'nuevaCiudad' es requerido." });
    }
    ciudad = nuevaCiudad;
    console.log(`\n[REST] Ciudad cambiada desde REST:`);
    procesarAccionRestComoMenu("2"); // Emular la acción en el menú
    res.json({ mensaje: `Ciudad cambiada a ${ciudad}` });
  } catch (error) {
    next(error);
  }
});

// Middleware para manejo global de errores
app.use((err, req, res, next) => {
  console.error("Error inesperado:", err.message || err);
  res.status(500).json({
    error: "Ha ocurrido un error inesperado. Por favor, inténtalo de nuevo más tarde.",
  });
});

// Middleware para manejar rutas no encontradas
app.use((req, res) => {
  res.status(404).json({
    error: "Ruta no encontrada. Verifica el endpoint solicitado.",
  });
});

// Iniciar el servidor
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Servidor REST ejecutándose en http://localhost:${PORT}`);
  console.log("\nPara usar la API REST de City Traffic Control:");
  console.log("- GET /estado_circulacion para verificar estado de circulación");
  console.log("- POST /cambiar_ciudad con JSON { 'nuevaCiudad': 'nombre' } para cambiar de ciudad");
  console.log("\nTambién se puede usar el Menú Interactivo:");
  mostrarMenu(); 
});
