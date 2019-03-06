import sys
import time
import pickle, json

from queue import Queue
from threading import Thread
from socket import socket, AF_INET, SOCK_STREAM, timeout


class ChannelTimeout(Exception):
    pass


class ChannelException(Exception):
    pass


class Channel(object):
    def __init__(self, index, address):
        self.index = index
        self.address = address
        self.sock = None

    def __str__(self):
        return f'Channel{self.index}: connected to {self.address}'
    
    def connect(self):
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
        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind(self.address)
        sock.listen(5)
        self.sock, _ = sock.accept()
        self.sock.settimeout(2 + self.index)
        return self.sock, _
    
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


class ClientChannel(Thread):
    def __init__(self, index, dest):
        super().__init__()
        self.index = index
        self.channel = None
        self.queue = Queue()
        self.address = ('localhost', BASEPORT + dest)
        print(f'chan{index}: ClientChannel INIT to {self.address}')
        
    def __str__(self):
        return f'chan{self.index}: ClientChannel to {self.address}'
    
    def send(self, message):
        self.queue.put(message)
        print(f'chan{self.index}: Put message on queue {message}')

    def run(self):
        while True:
            if not self.channel:
                self.channel = Channel(self.index, self.address)
                try:
                    self.channel.connect()
                except:
                    self.channel = None
                    print(f'chan{self.index}: Exception connecting to {self.address}:', sys.exc_info()[1])
                    time.sleep(2)
                    continue
                print(f'chan{self.index}: Created client channel to: {self.address}')
            message = self.queue.get()
            print(f'chan{self.index}: Sending {message}')
            try:
                self.channel.send(message)
            except:
                print(f'chan{self.index}: Exception in send({message})')
                self.channel = None
        

class ServerSocket(Thread):
    def __init__(self, server, channel):
        super().__init__()
        self.server = server
        self.channel = channel

    def run(self):
        while True:
            try:
                message = self.channel.receive()
            except ChannelTimeout:
                message = ChannelTimeout()
                time.sleep(1)
            except ChannelException:
                message = ChannelException(sys.exc_info())
                time.sleep(1)
            self.server.queue.put(message)
                
            
BASEPORT = 9000        
class ServerChannel(Thread):
    def __init__(self, server, index):
        super().__init__()
        self.server = server
        self.index = index
        self.port = BASEPORT + index
        
    def run(self):
        while True:
            channel = Channel(self.index, ('localhost', self.port))
            try:
                self.sock, _ = channel.accept()
            except:
                print(f'chan{self.index}: Exception in accept()', sys.exc_info())
                time.sleep(2)
                continue
            print(f'chan{self.index}: Created Server on port: {self.port} from {self.sock.getpeername()}')
            s = ServerSocket(self.server, channel)
            s.start()
