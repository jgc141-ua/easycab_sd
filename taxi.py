class Taxi():
    def __init__(self, fila, col):
        self.fila=fila
        self.col=col
        
    def getFila (self):
        return self.fila
    
    def getCol (self):
        return self.col
    
    def mover (self, fila, col):
        self.fila = fila
        self.col = col