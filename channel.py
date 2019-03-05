import sys
import time
import pickle, json

from socket import socket, AF_INET, SOCK_STREAM, timeout


class ChannelTimeout(Exception):
    pass


class ChannelException(Exception):
    pass


class Channel(object):
    def __init__(self, address):
        self.address = address
        self.sock = None

    def connect(self):
        self.reconnect = self._connect

    def _connect(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.settimeout(1)
        self.sock.connect(self.address)

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
        sock.settimeout(1)
        sock.bind(self.address)
        sock.listen(1)
        self.sock, _ = sock.accept()
        self.sock.settimeout(1)
        
    def send_data(self, data):
        if not self.sock:
            self.reconnect()
        size = len(data)
        header = b'%10d' % size
        self.sock.sendall(header)
        self.sock.sendall(data)

    def receive_data(self):
        if not self.sock:
            self.reconnect()
        header = self._receive_size(10)
        size = int(header)
        data = self._receive_size(size)
        return data

    def send(self, message):
        obj = pickle.dumps(message)
        self.send_data(obj)

    def ack(self, message):
        obj = pickle.dumps(message)
        self.send_data(obj)
        
    def receive(self):
        try:
            obj = self.receive_data()
            message = pickle.loads(obj)
            return message
        except timeout:
            raise ChannelTimeout(sys.exc_info())
        except IOError:
            raise ChannelException(sys.exc_info())
