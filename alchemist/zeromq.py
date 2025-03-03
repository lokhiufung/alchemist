import time
from typing import Any
import zmq

from alchemist.logger import get_logger


class ZeroMQ:
    """
    A class to manage ZeroMQ communication for sending and receiving messages.

    Attributes:
        name (str): The name of the ZeroMQ instance.
        _logger: Logger instance for debug and error messages.
        _send_port (int): The port used for sending messages.
        _recv_ports (list[int]): The list of ports used for receiving messages.
    """

    def __init__(self, name: str):
        """
        Initializes a ZeroMQ instance with the given name.

        Args:
            name (str): The name of the ZeroMQ instance.
        """
        self.name = name.lower()
        self._logger = get_logger('zmq', console_logger_lv='info', file_logger_lv='debug')
        self._send_port = None
        self._recv_ports = []
        self._sender_context = zmq.Context()
        self._sender = self._sender_context.socket(zmq.PUB)
        self._receiver_context = zmq.Context()
        self._receiver = self._receiver_context.socket(zmq.SUB)
        self._receiver.setsockopt(zmq.SUBSCRIBE, b'')
        self._poller = zmq.Poller()

    def __str__(self) -> str:
        """
        Returns a string representation of the ZeroMQ instance.

        Returns:
            str: A string describing the ZeroMQ instance.
        """
        return f'ZeroMQ({self.name}_zmq): send using port {self._send_port}, receive from ports {self._recv_ports}'

    def _set_logger(self, logger):
        """
        Sets the logger for the ZeroMQ instance.

        Args:
            logger: A logger instance.
        """
        self._logger = logger

    def start(self, logger, send_port: int, recv_ports: list[int]) -> None:
        """
        Starts the ZeroMQ instance with the specified ports and logger.

        Args:
            logger: A logger instance.
            send_port (int): The port for sending messages.
            recv_ports (list[int]): The ports for receiving messages.
        """
        self._set_logger(logger)
        self._poller.register(self._receiver, zmq.POLLIN)
        self._send_port = send_port
        self._recv_ports = recv_ports
        self._logger.debug(f'{self.name}_zmq set send_port to {self._send_port}, recv ports to {self._recv_ports}')
        if self._send_port:
            send_address = f"tcp://127.0.0.1:{self._send_port}"
            self._sender.bind(send_address)
            self._logger.debug(f'{self.name}_zmq sender binded to {send_address}')
        for port in self._recv_ports:
            recv_address = f"tcp://127.0.0.1:{port}"
            self._receiver.connect(recv_address)
            self._logger.debug(f'{self.name}_zmq receiver connected to {recv_address}')
        self._logger.debug(f'{self.name}_zmq started')
        time.sleep(1)  # Give ZeroMQ some prep time to prevent missed messages.

    def stop(self) -> None:
        """
        Stops the ZeroMQ instance, closing all sockets and contexts.
        """
        self._poller.unregister(self._receiver)

        # Terminate sender
        send_address = f"tcp://127.0.0.1:{self._send_port}"
        self._sender.unbind(send_address)
        self._logger.debug(f'{self.name}_zmq sender unbound from {send_address}')
        self._sender.close()
        time.sleep(0.1)
        self._sender_context.term()

        # Terminate receiver
        for port in self._recv_ports:
            recv_address = f"tcp://127.0.0.1:{port}"
            self._receiver.disconnect(recv_address)
            self._logger.debug(f'{self.name}_zmq receiver disconnected from {recv_address}')
        self._receiver.close()
        time.sleep(0.1)
        self._receiver_context.term()
        self._logger.debug(f'{self.name}_zmq stopped')
        self._logger = None

    def send(self, channel: int, topic: int, info: Any, receiver: str = '') -> None:
        """
        Sends a message to receivers/subscribers.

        Args:
            channel (int): The communication channel.
            topic (int): The topic identifier.
            info (Any): The message content.
            receiver (str, optional): A specific receiver's name. Defaults to ''.
        """
        if not self._sender.closed:
            msg = (time.time(), receiver, channel, topic, info)
            self._sender.send_pyobj(msg)
            self._logger.debug(f'{self.name}_zmq sent {msg}')
        else:
            self._logger.debug(f'{self.name}_zmq sender is closed, cannot send message {msg}')

    def recv(self):
        """
        Receives a message from the ZeroMQ receiver.

        Returns:
            tuple: A tuple containing the channel, topic, and info.
        """
        try:
            events = self._poller.poll(0)  # 0-second timeout
            if events and not self._receiver.closed:
                msg = self._receiver.recv_pyobj()
                ts, receiver, channel, topic, info = msg
                # TODO monitor latency using ts
                if not receiver or receiver.lower() == self.name:
                    self._logger.debug(f'{self.name}_zmq recv {msg}')
                    return channel, topic, info
        except KeyboardInterrupt:
            # need to close the sockets and terminate the contexts
            # otherwise, zmq will probably be stuck at somewhere in poll()
            # and can't exit the program
            self.stop()
            raise KeyboardInterrupt
        except:
            self._logger.exception(f'{self.name}_zmq recv exception:')
    
    # TODO, monitor zmq latency
    def _monitor(self):
        pass