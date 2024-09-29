class Mapa():
    def __init__(self, alto, ancho):
        self.alto = alto
        self.ancho = ancho
        self.mapa = ["." * self.ancho] * self.alto
    
    def getAlto (self):
        return self.alto
    
    def getAncho (self):
        return self.ancho
    
    def mostrarMapa(self):
        salida = "    "

        for col in range(self.ancho):
            salida += f"{col + 1}"
            if col + 1 < 10:
                salida += "  "
            else:
                salida += " "
        salida += "\n"

        for fila in range(self.alto):
            if fila + 1 < 10:
                salida += f" {fila + 1}  "
            else:
                salida += f"{fila + 1}  "

            for col in range(self.ancho):               
                salida += f"{self.mapa[fila][col]}  "

            salida += "\n"

        return salida