import sys
import time

from channel import Channel, ChannelTimeout, ChannelException
from state import State
from events import TimeoutEvent, ExceptionEvent, LogEvent, AckEvent, Event

class Node(object):
    def __init__(self, address):
        self.address = address
        self.channel = Channel(self.address)
        self.serve = True
        self.state = State(self)
        
    def accept(self):
        while True:
            try:
                return self.channel.accept()
            except:
                time.sleep(2)
            
    def connect(self):
        while True:
            try:
                return self.channel.connect()
            except:
                time.sleep(2)
        
    def receive(self):
        try:
            event = self.channel.receive()
            return event
        except ChannelTimeout:
            return TimeoutEvent()
        except ChannelException:
            return ExceptionEvent(sys.exc_info())
    
    def ack(self, event):
        return self.channel.ack(event)
    
    def send(self, event):
        event.seq = Event.seq
        self.channel.send(event)
        Event.seq += 1
        return event.seq
        
    def call(self, event):
        seq = self.send(event)
        ack = self.receive()
        if ack.request != 'ack':
            print(f'Bad ack event: {ack.request}')
            return False
        if seq != ack.seq:
            print(f'Bad seq: {seq} != {ack.seq}')
            return False
        return True


class Server(Node):
    def run(self):
        while self.serve:
            self.accept()
            while True:
                event = self.receive()
                handle = getattr(self.state, event.request)
                reply = handle(event)
                if reply:
                    self.ack(reply)

class Client(Node):
    def set_count(self, count):
        self.count = count
        
    def run(self):
        self.connect()
        for i in range(self.count):
            log = LogEvent(i, 'Some log message %d' % i)
            self.send(log)
            if debug: print(f'Sent {log}')
            ack = self.receive()
            if isinstance(ack, TimeoutEvent):
                continue
            if not isinstance(ack, AckEvent):
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
        server = Server(('localhost', port))
        server.run()
    else:
        client = Client(('', port))
        client.set_count(10)
        client.run()
    print('Time: ', time.time() - start)
        
