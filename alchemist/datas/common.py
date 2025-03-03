from datetime import datetime


class Bar:
    def __init__(self, ts: datetime, open_: float, high: float, low: float, close: float, volume: float):
        self.ts = ts
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def update(self, ts: datetime, open_: float, high: float, low: float, close: float, volume: float):
        self.ts = ts
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __str__(self):
        return f'Bar(ts={self.ts}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, volume={self.volume})'
    
    def __repr__(self):
        return f'Bar(ts={self.ts}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, volume={self.volume})'


class Tick:
    def __init__(self, ts, price, size):
        self.ts = ts
        self.price = price
        self.size = size
    
    def __str__(self):
        return f"Tick(ts={self.ts}, price={self.price}, size={self.size})" 
    
    def __repr__(self):
        return f"Tick(ts={self.ts}, price={self.price}, size={self.size})"
