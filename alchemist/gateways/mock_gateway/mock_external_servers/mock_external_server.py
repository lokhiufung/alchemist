import time

from alchemist.gateways.mock_gateway.time_bar_generators.time_bar_generator import TimeBarGenerator
from alchemist.standardized_messages import create_bar_message, create_balance_update_message


class MockExternalServer:
    def __init__(self, zmq, subscriptions, products, msg_interval=0.5):
        self.zmq = zmq
        self.subscriptions = subscriptions
        self.products = products
        self.msg_interval = msg_interval
        self.time_bar_generators = {product.name: TimeBarGenerator() for product in products}

    def run_server(self):

        time.sleep(1)

        self.zmq.send(
            channel=4,
            topic=2,
            info=('test_gateway', True)
        )
        msg = create_balance_update_message(
            ts=time.time(),
            gateway='test_gateway',
            balance_update={
                'USD': 1000000.0
            }
        ) 
        self.zmq.send(*msg)

        while True:
            for product in self.products:
                # subscription = random.choice(self.subscriptions)
                # current_bar_time should be the int timestamp of the current bar
                time_bar_generator = self.time_bar_generators[product.name]
                bar = time_bar_generator.next()
                zmq_msg = create_bar_message(bar['ts'], 'test_gateway', product.exch, product.name, time_bar_generator.freq, 
                                            open_=bar['open'], 
                                            high=bar['high'], 
                                            low=bar['low'], 
                                            close=bar['close'], 
                                            volume=bar['volume'])
                self.zmq.send(*zmq_msg)
                
            time.sleep(self.msg_interval)
