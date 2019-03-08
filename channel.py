import traceback
import sys
import time
import logging
import pickle, json
from random import randint

from queue import Queue
from threading import Thread
from socket import socket, AF_INET, SOCK_STREAM, timeout, SOL_SOCKET, SO_REUSEADDR

logger = logging.getLogger(__name__)

class ChannelTimeout(Exception):
    pass


class ChannelException(Exception):
    pass


class Channel(object):
    def __init__(self, node, index, address):
        self.node = node
        self.index = index
        self.address = address
        self.sock = None

    def __str__(self):
        return f'chan{self.node.index}: Channel{self.index}: connected to {self.address}'

    def __del__(self):
        if self.sock:
            self.sock.close()
            self.sock = None
        
    def up(self):
        return self.sock
    
    def connect(self):
        self.reconnect = self._connect
            
    def _connect(self):
        self.retry = 0
        while self.retry < 5:
            try:
                self.sock = socket(AF_INET, SOCK_STREAM)
                self.sock.connect(self.address)
                return
            except:
                self.retry += 1
                logger.warning(f'{self}: Connect failed to {self.address} ({self.retry}): {sys.exc_info()[1]}')
                # time.sleep(1 + self.retry)
        logger.error(f'{self}: Gave up trying to connect to {self.address}')

    def _receive_size(self, size):
        message = bytearray()
        while size > 0:
            fragment = self.sock.recv(size)
            if not fragment:
                self.sock = None
                raise IOError('Network error')
            message.extend(fragment)
            size -= len(fragment)
        return message

    def accept(self):
        self.reconnect = self._accept
            
    def _accept(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock.bind(self.address)
        sock.listen(5)
        sock.settimeout(1)
        self.sock, _ = sock.accept()
        self.sock.settimeout(1)
    
    def send_data(self, data):
        self.reconnect()
        size = len(data)
        header = b'%10d' % size
        self.sock.sendall(header)
        self.sock.sendall(data)
        self.sock.close()
        self.sock = None

    def receive_data(self):
        self.reconnect()
        header = self._receive_size(10)
        size = int(header)
        data = self._receive_size(size)
        self.sock.close()
        self.sock = None
        return data

    def send(self, message):
        obj = pickle.dumps(message)
        try:
            self.send_data(obj)
        except:
            logger.error(f'chan{self.node.index}: Exception in send to {self.address}: {sys.exc_info()[1]}')
            raise ChannelException(sys.exc_info())

    def receive(self):
        try:
            obj = self.receive_data()
            message = pickle.loads(obj)
            # logger.debug(f'chan{self.node.index}: Got message: {message}')
            return message
        except timeout:
            raise ChannelTimeout(sys.exc_info())
        except IOError:
            logger.errro(f'chan{self.node.index}: Exception in receive from {self.address}: {sys.exc_info()[1]}')
            time.sleep(randint(2,9))
            if self.sock:
                self.sock.close()
            self.sock = None
            raise ChannelException(sys.exc_info())


class ClientChannel(Channel):
    def __init__(self, node, index, dest):
        super().__init__(node, index, dest)
        self.reconnect = self._connect
        

class ServerChannel(Channel, Thread):
    def __init__(self, node, index, address):
        super().__init__(node, index, address)
        self.reconnect = self._accept
