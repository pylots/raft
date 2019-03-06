import os
import sys
import time
import json

from threading import Thread
from queue import Queue, Empty
from channel import ServerChannel, ClientChannel, ChannelTimeout, ChannelException
from state import RestartState
from messages import TimeoutMessage, ExceptionMessage, LogMessage, AckMessage, Message


class NodeController:
    def __init__(self, count):
        self.count = count

class Node(Thread):
    def __init__(self, control, index=0):
        super().__init__()
        self.control = control
        self.index = index
        self.serve = True
        self.state = RestartState(self)
        
    def connect(self, dst):
        self.dest = dst
        while True:
            try:
                return self.channel.connect()
            except:
                time.sleep(2)
        
    def receive(self):
        try:
            message = self.queue.get(timeout=1)
        except Empty:
            message = TimeoutMessage()
        if isinstance(message, ChannelTimeout):
            message = TimeoutMessage()
        if isinstance(message, ChannelException):
            message = ExceptionMessage(Node)
        return message
    
    def send(self, message):
        message.seq = Message.seq
        dest = message.mdest
        if dest == self.index:
            print(f'node{self.index}: Talking to myself')
            return
        if not dest in self.nodes:
            print(f'node{self.index}: Going to created channel to {dest}')
            self.nodes[dest] = ClientChannel(self.index, dest)
            self.nodes[dest].start()
            print(f'node{self.index}: Created channel to {dest}')
        channel = self.nodes[dest]
        print(f'node{self.index}: Sending {message} to {channel}')
        channel.send(message)
        Message.seq += 1
        return message.seq
        

class Config:
    def __init__(self, name):
        self.path = name
        if os.path.isfile(self.path):
            self.config = json.loads(open(self.path, 'r').read())
        else:
            self.config = {}

    def __getitem__(self, key):
        if not key in self.config:
            self.config[key] = 0
        return self.config[key]
            
    def __setitem__(self, key, val):
        self.config[key] = val

    def save(self):
        open(self.path, 'w+').write(json.dumps(self.config))
        
    def __str__(self):
        return str(self.config)
    

class Server(Node):
    def init(self):
        self.config = Config(f'node{self.index}.json')
        print(f'config={self.config}')
        self.queue = Queue()
        self.channel = ServerChannel(self, self.index)
        self.channel.start()
        self.nodes = {}
        
    def run(self):
        print(f'node{self.index}: Server ready')
        while True:
            message = self.receive()
            handle = getattr(self.state, message.request)
            reply = handle(message)
            if reply:
                self.send(reply)
                self.config.save()

    def close(self):
        self.config.save()


class Client(Node):
    def set_count(self, count):
        self.count = count
        
    def run(self):
        self.connect('node0')
        for i in range(self.control.count):
            log = LogMessage(i, 'Some log message %d' % i)
            self.send(log)
            if debug: print(f'Sent {log}')
            ack = self.receive()
            if isinstance(ack, TimeoutMessage):
                continue
            if not isinstance(ack, AckMessage):
                print(f'Got bad ACK: {ack.request}')
            if ack.seq != log.seq:
                print(f'Bad seq no: Expected {log.seq}, got {ack.seq}')
                
        
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
        client.set_count(10)
        client.run()
    print('Time: ', time.time() - start)
        
