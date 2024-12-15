window.addEventListener("load", async () => {
    const loader = document.getElementById("loader");
    
    setTimeout(() => {
        loader.classList.add("loader-hidden");
    }, 3000);
    
    // Remover el loader del DOM después de la transición
    loader.addEventListener("transitionend", () => {
        loader.remove();
    });
});

function getCard(header, id, service, active) {
    let classCard = "card"
    if (active == "KO") {
        classCard = "card event"

    }

    if (header == "CUSTOMER") {
        service = `Destino ${service}`;

    }

    card = `<div class="${classCard}"> <div class="card-header">${header}</div> <div class="card-id">${id}</div> <div class="card-service">${service}</div> </div>`;
    return card;

}

async function getTaxis() {
    try {
        const response = await fetch("https://192.168.24.1:5151/taxis", {
            method: "GET",
            headers: { "Content-Type": "application/json" }
        });

        if (!response.ok) {
            throw new Error("ERROR!! Al recuperar la información de la API (Taxis)");
        }

        const text = await response.text();
        const busiesTaxisDiv = document.getElementById("busiesTaxis");
        const notBusiesTaxisDiv = document.getElementById("notBusiesTaxis");

        try {
            const taxis = JSON.parse(text);
            busiesTaxisDiv.innerHTML = "";
            notBusiesTaxisDiv.innerHTML = "";
            for(let i = 0; i < taxis.length; i++) {
                let taxi = taxis[i];
                let taxiID = taxi[0];
                let active = taxi[1];
                let service = taxi[2];
                console.log([taxiID, active, service]);

                let card = getCard("TAXI", taxiID, service, active);
                if (service == "Parado") {
                    notBusiesTaxisDiv.innerHTML += card;

                } else {
                    busiesTaxisDiv.innerHTML += card;

                }
            }
        } catch (parseError) {
            console.error("Error parseando JSON (Taxis):", parseError);
        }

    } catch (error) {
        console.error("Error leyendo los taxis:", error);
    }
}

async function getCustomers() {
    try {
        const response = await fetch("https://192.168.24.1:5151/customers", {
            method: "GET",
            headers: { "Content-Type": "application/json" }
        });

        if (!response.ok) {
            throw new Error("ERROR!! Al recuperar la información de la API (Customers)");
        }

        const text = await response.text();
        const servicedCustomers = document.getElementById("servicedCustomers");
        const notServicedCustomers = document.getElementById("notServicedCustomers");

        try {
            const customers = JSON.parse(text);
            servicedCustomers.innerHTML = "";
            notServicedCustomers.innerHTML = "";
            for(let i = 0; i < customers.length; i++) {
                let customer = customers[i];
                let customerID = customer[0];
                let destination = customer[1];
                let active = customer[2];
                console.log([customerID, destination, active]);

                let card = getCard("CUSTOMER", customerID, destination, active);
                if (active == "KO") {
                    notServicedCustomers.innerHTML += card;

                } else {
                    servicedCustomers.innerHTML += card;

                }
            }
        } catch (parseError) {
            console.error("Error parseando JSON (Taxis):", parseError);
        }

    } catch (error) {
        console.error("Error leyendo los taxis:", error);
    }
}

//region LOGS
let ID = 0
let fistLog = false
// Función para leer los logs
async function getLogs() {
    try {
        const response = await fetch("https://192.168.24.1:5151/logs", {
            method: "GET",
            headers: { "Content-Type": "application/json" }
        });

        if (!response.ok) {
            throw new Error("ERROR!! Al recuperar la información de la API");
        }

        const text = await response.text();
        const logsDiv = document.getElementById("logs");

        try {
            const logsArray = JSON.parse(text);
            const formattedLogs = logsArray.flat().map(log => `${++ID} - ${log.trim()} <br><br>`);
            if (formattedLogs != "" && !fistLog) {
                let no_logs = document.getElementById("no-logs");
                no_logs.remove();
                fistLog = true;
            }

            logsDiv.innerHTML += formattedLogs;
            logsDiv.scrollTop = logsDiv.scrollHeight;
        } catch (parseError) {
            console.error("Error parseando JSON:", parseError);
        }

    } catch (error) {
        console.error("Error leyendo logs:", error);
    }
}

// Actualizar logs cada segundo
setInterval(getLogs, 1000);
setInterval(getTaxis, 1000);
setInterval(getCustomers, 1000);

// Leer logs inmediatamente al cargar
getLogs();