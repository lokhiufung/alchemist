import time
from threading import Thread

import ray

from alchemist.gateways.base_gateway import BaseGateway
from alchemist.standardized_messages import create_order_update_message, create_balance_update_message, create_position_update_message, create_position_update
from alchemist.gateways.mock_gateway.mock_external_servers.mock_external_server import MockExternalServer


@ray.remote
class MockGateway(BaseGateway):
    NAME = 'mock_gateway'

    def _run_connection_server(self):
        data_server = MockExternalServer(self._zmq, self.subscriptions, self.products, msg_interval=1.5)
        # try to connect to the server
        self._connection_thread = Thread(
            name='mock_gateway',
            target=data_server.run_server, 
            daemon=True
        )
        self._connection_thread.start()

    def _on_connected(self):
        self._is_connected = True

    def _reject_order(self, order_dict):
        zmq_msg = create_order_update_message(
            ts=time.time(),
            gateway=self.NAME,
            strategy=order_dict['data']['strategy'],
            exch=order_dict['data']['product']['exch'],
            pdt=order_dict['data']['product']['name'],
            oid=order_dict['data']['oid'],
            status='INTERNALLY_REJECTED',
            average_filled_price=None,
            last_filled_price=None,
            last_filled_size=None,
            amend_price=None,
            amend_size=None,
            create_ts=order_dict['data']['create_ts'],
            target_price=order_dict['data']['price'],
        )
        # 1. reject the order
        self._zmq.send(*zmq_msg)

    def _immediately_fill_order(self, order_dict):
        zmq_msg = create_order_update_message(
            ts=time.time(),
            gateway=self.NAME,
            strategy=order_dict['data']['strategy'],
            exch=order_dict['data']['product']['exch'],
            pdt=order_dict['data']['product']['name'],
            oid=order_dict['data']['oid'],
            status='FILLED',
            average_filled_price=order_dict['data']['price'],
            last_filled_price=order_dict['data']['price'],
            last_filled_size=order_dict['data']['size'],
            amend_price=None,
            amend_size=None,
            create_ts=order_dict['data']['create_ts'],
            target_price=order_dict['data']['price'],
        )
        # 1. immediately fill
        self._zmq.send(*zmq_msg)
        # 2. send position update
        position_update = create_position_update(
            pdt=order_dict['data']['product']['name'],
            side=order_dict['data']['side'],
            size=order_dict['data']['size'],
            last_price=order_dict['data']['price'],
            average_price=order_dict['data']['price'],
            realized_pnl=0.0,
            unrealized_pnl=0.0
        )            

        zmq_msg = create_position_update_message(
            ts=time.time(),
            gateway=self.NAME,
            exch=order_dict['data']['product']['exch'],
            position_update=position_update,
        )
        self._zmq.send(*zmq_msg)

    def _handle_market_order(self, order_dict):
        self._immediately_fill_order(order_dict)

    def _handle_limit_order(self, order_dict):
        self._immediately_fill_order(order_dict)

    def place_order(self, order_dict):
        order_type = order_dict['data']['order_type']
        
        if order_type == 'MARKET':
            self._handle_market_order(order_dict)
        elif order_type == 'LIMIT':
            self._handle_limit_order(order_dict)
        else:
            # send a internal rejection in message
            # TODO
            pass

