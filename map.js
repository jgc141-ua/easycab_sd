function createClearMap() {
    const map = document.getElementById("map");
    map.innerHTML = "";
    
    for (let i = 0; i < 20; i++) {
        const col = document.createElement("div");
        col.className = "col";
        
        for (let j = 0; j < 20; j++) {
            const mapCell = document.createElement("div");
            mapCell.className = "mapCell";
            mapCell.dataset.x = j;
            mapCell.dataset.y = i;
            
            const objects = document.createElement("div");
            objects.className = "objects";
            
            mapCell.appendChild(objects);
            col.appendChild(mapCell);
        }
        map.appendChild(col);
    }
    
    return map;

}

async function createObject(objectID, x, y) {
    const cell = document.querySelector(`[data-x="${x}"][data-y="${y}"]`);
    if (cell) {
        cell.innerHTML = "";

        const object2Create = document.createElement("p");

        if (/[A-Z]/.test(objectID)) {
            cell.classList.add("locations");
        } else if (/^[0-9]$/.test(objectID) || (objectID.length == 2 && objectID[1] != "!")) {
            cell.classList.add("taxi");
        } else if (/[a-z]/.test(objectID)) {
            cell.classList.add("customer");
        } else {
            cell.classList.add("taxiEvent");
        }

        object2Create.textContent = objectID;

        cell.appendChild(object2Create);

    }
}

async function getMap() {
    const response = await fetch("https://192.168.24.1:5151/objects", {
        method: "GET",
        headers: { "Content-Type": "application/json" }
    });

    if (!response.ok) {
        throw new Error("ERROR!! Al recuperar la información de la API (Taxis)");
    }

    createClearMap();

    const text = await response.text();

    try {
        const objects = JSON.parse(text);
        for(let i = 0; i < objects.length; i++) {
            object = objects[i];
            objectID = object[0];
            objectX = object[1];
            objectY = object[2];

            createObject(objectID, objectX, objectY);

        }
    } catch (parseError) {
        console.error("Error parseando JSON (Map Objects):", parseError);
    }

}

setInterval(getMap, 1000);

createClearMap();
getMap();