import sys
import time
import logging
import pickle
import socket
from random import randint

from threading import Thread

logger = logging.getLogger(__name__)

class ChannelTimeout(Exception):
    pass


class ChannelException(Exception):
    pass


class Channel(object):
    def __init__(self, index, sock=None):
        self.index = index
        self.sock = sock

    def __str__(self):
        return f'chan{self.index}'

    def __del__(self):
        logger.debug(f'{self}: GC {self.sock}')
        if self.sock:
            self.sock.close()
        self.sock = None

    def connect(self, address):
        self.retry = 0
        while self.retry < 5:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                logger.debug(f'{self}: Connecting to {address}')
                self.sock.connect(address)
                self.retry = 0
                logger.debug(f'{self}: Got a connection to {address}')
                return
            except OSError as e:
                self.retry += 1
                logger.warning(f'{self}: Connect failed to {address} ({self.retry}): {e}')
                time.sleep(1 + self.retry)
        logger.error(f'{self}: Gave up trying to connect to {address}')

    def _receive_size(self, size):
        message = bytearray()
        while size > 0:
            fragment = self.sock.recv(size)
            if not fragment:
                raise IOError(f'{self}: Network error')
            message.extend(fragment)
            size -= len(fragment)
        return message

    def accept(self, address):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(address)
            sock.listen(10)
            self.sock, _ = sock.accept()
            return Channel(self.index, self.sock)
        except socket.timeout as e:
            raise ChannelTimeout(e)
        except IOError as e:
            logger.error(f'{self}: Exception in bind/listen/accept: {e}')
            raise ChannelException(e)

    def send_data(self, data):
        size = len(data)
        header = b'%10d' % size
        self.sock.sendall(header)
        self.sock.sendall(data)

    def receive_data(self):
        header = self._receive_size(10)
        size = int(header)
        data = self._receive_size(size)
        return data

    def send(self, message):
        obj = pickle.dumps(message)
        try:
            self.send_data(obj)
        except Exception as e:
            logger.error(f'{self}: Exception in send: {e}')
            raise ChannelException(e)

    def receive(self):
        try:
            obj = self.receive_data()
            message = pickle.loads(obj)
            logger.debug(f'{self}: Got message: {message}')
            return message
        except socket.timeout as e:
            raise ChannelTimeout(e)
        except IOError as e:
            logger.error(f'{self}: Exception in receive: {e}')
            raise ChannelException(e)
