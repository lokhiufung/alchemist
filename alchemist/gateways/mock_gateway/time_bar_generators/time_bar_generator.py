import random
import math
from datetime import datetime, timedelta
from collections import deque


class TimeBarGenerator:
    def __init__(self, base_ts=datetime(1996, 12, 17, 9, 30, 0), base_price=100.0, freq='1m', volatility=0.02, drift=0.001, dt=1/60, buffer_size=10):
        if freq != '1m':
            raise ValueError('Only `1m` frequency is supported')
        self.base_ts = base_ts
        self.base_price = base_price
        self.freq = freq
        self.volatility = volatility
        self.drift = drift
        self.dt = dt
        # initialize the current time
        self.current_time = base_ts
        self.current_price = base_price
        
        self.buffer_size = buffer_size
        self.buffer = deque(maxlen=self.buffer_size)

    def next(self):
        bar = {
            'ts': self.current_time.timestamp(),
            'open': self.current_price, 
            'high': self.current_price + random.uniform(0, 2*self.volatility), 
            'low': self.current_price - random.uniform(0, 2*self.volatility), 
            'close': self.current_price + random.uniform(-self.volatility, self.volatility), 
            'volume': random.uniform(900, 1100)
        }
        self.current_time = self.current_time + timedelta(minutes=1)
        # Simulate a Brownian motion
        dW = random.gauss(0, math.sqrt(self.dt))
        self.current_price = self.current_price * math.exp((self.drift - 0.5 * self.volatility**2) * self.dt + self.volatility * dW)

        self.buffer.append(bar)

        return bar
    
    def get_previous_bars(self, n=1):
        return list(self.buffer)[-n:]
