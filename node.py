import logging
import os
import sys
import time
import json

from threading import Thread
from queue import Queue
from channel import ClientChannel, ServerChannel, ChannelTimeout, ChannelException
from state import RestartState
from messages import TimeoutMessage, ExceptionMessage, LogMessage, AckMessage, Message

BASEPORT = 9000

logger = logging.getLogger(__name__)


class NodeController:
    def __init__(self, count):
        self.count = count

        
class Node:
    def __init__(self, control, index=0):
        super().__init__()
        self.control = control
        self.index = index
        self.channel = None
        self.state = RestartState(self)
        self.currentTerm = 0
        self.votedForm = 0
        
    def receive(self):
        try:
            message = self.channel.receive()
        except ChannelTimeout:
            message = TimeoutMessage()
        except ChannelException:
            message = ExceptionMessage(Node)
        return message
    
    def send(self, message):
        dest = message.mdest
        if dest == self.index:
            logger.warning(f'node{self.index}: Talking to myself')
            return
        logger.debug(f'node{self.index}: Going to created channel to {dest}')
        channel = ClientChannel(self, self.index, ('localhost', BASEPORT + dest))
        channel.connect()
        # logger.debug(f'node{self.index}: Sending {message} to {channel}')
        try:
            channel.send(message)
        except ChannelException:
            logger.error(f'node{self.index}: Failed sending to {dest}: {sys.exc_info()[1]}')
            return
        

class Config:
    def __init__(self, name):
        self.path = name
        if os.path.isfile(self.path):
            self.config = json.loads(open(self.path, 'r').read())
        else:
            self.config = {}

    def __str__(self):
        return str(self.config)

    def __getitem__(self, key):
        if not key in self.config:
            self.config[key] = 0
        return self.config[key]
            
    def __setitem__(self, key, val):
        self.config[key] = val

    def save(self):
        open(self.path, 'w+').write(json.dumps(self.config))
        
    
class ServerQueue(Thread):
    def __init__(self, node):
        super().__init__()
        self.node = node
        self.index = node.index
        self.queue = Queue()
        self.channel = ServerChannel(self, self.index, self.node.address)
        
    def run(self):
        while True:
            try:
                message = self.channel.receive()
                # logger.debug(f'{self.index}: Received {message}')
            except ChannelTimeout:
                message = TimeoutMessage()
            except ChannelException:
                message = ExceptionMessage(sys.exc_info())
            self.queue.put(message)

    def receive(self):
        return self.queue.get()


class ClientQueue(Thread):
    def __init__(self, node):
        super().__init__()
        self.node = node
        self.index = node.index
        self.queue = Queue()
        
    def run(self):
        logger.info(f'{self.index}: ClientQueue running')
        while True:
            message = self.queue.get()
            try:
                channel = ClientChannel(self.node, self.index, ('localhost', BASEPORT + message.mdest))
                channel.connect()
                channel.send(message)
            except ChannelTimeout:
                message = TimeoutMessage()
            except ChannelException:
                message = ExceptionMessage(sys.exc_info())

    def send(self, message):
        return self.queue.put(message)



class Server(Node, Thread):
    def init(self):
        self.config = Config(f'node{self.index}.json')
        logger.info(f'config={self.config}')
        self.address = ('localhost', BASEPORT + self.index)
        self.queue = ServerQueue(self)
        self.queue.start()
        self.out = ClientQueue(self)
        self.out.start()
        
    def run(self):
        logger.info(f'node{self.index}: Server ready on {self.address}')
        while True:
            message = self.queue.receive()
            # logger.debug(f'{self.index}: Server received: {message}')
            handle = getattr(self.state, message.request)
            reply = handle(message)
            if reply:
                self.send(reply)
                self.config.save()

    def send(self, m):
        if self.index == m.mdest:
            logger.error(f'{self.index}: Sending {message} to myself {m.mdest}')
            return
        self.out.send(m)
        
    def close(self):
        self.config.save()


class Client(Node):
    def init(self, count=10):
        self.count = count
        self.address = ('localhost', BASEPORT + self.index)
        self.channel = ClientChannel(self, self.index, self.address)
        
    def run(self):
        self.channel.connect()
        for i in range(self.control.count):
            log = LogMessage(i, 'Some log message %d' % i)
            self.send(log)
            ack = self.channel.receive()
            if isinstance(ack, TimeoutMessage):
                continue
            if not isinstance(ack, AckMessage):
                print(f'Got bad ACK: {ack.request}')
                
        
debug = False
if __name__ == "__main__":
    port = 8888
    serve = False
    if 'serve' in sys.argv:
        serve = True
    if 'debug' in sys.argv:
        debug = True
    start = time.time()
    if serve:
        nc = NodeController(5)
        print(f'count={nc.count}')
        for index in range(nc.count):
            print(f'node{index}: Create server on {index}')
            server = Server(nc, index)
            server.init()
            server.start()
    else:
        client = Client(None)
        client.init(10)
        client.run()
    print('Time: ', time.time() - start)
        
