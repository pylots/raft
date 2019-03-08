import logging
import os
import sys
import time
import json
import copy
from random import randint

from threading import Thread
from queue import Queue
from channel import ClientChannel, ServerChannel, ChannelTimeout, ChannelException
from state import RestartState
from messages import TimeoutMessage, ExceptionMessage, LogMessage, AckMessage

BASEPORT = 9000

logging.basicConfig(
    filename='raft.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname).3s [%(name)s:%(lineno)s] %(message)s',
    datefmt='%y%m%d %H%M%S'
)


logger = logging.getLogger(__name__)

blue = '\001\033[34m\002'
green = '\001\033[32m\002'
red = '\001\033[31m\002'
reset = '\001\033[0m\002'
bold = '\001\033[1m\002'
bunder = '\001\033[4m\002'
eunder = '\001\033[24m\002'
colormap = {
    'R': reset, 'F': green, 'C': red, 'L': blue,
}


class NodeController:
    def __init__(self, count):
        self.status_text = None
        self.count = count
        self.nodes = []
        for index in range(self.count):
            node = Server(self, index)
            self.nodes.append(node)

    def init(self):
        logger.info('*** Starting ***')
        for node in self.nodes:
            node.init()

    def start(self):
        for node in self.nodes:
            node.start()

    def get_node(self, index):
        if index < 0 or index >= len(self.nodes):
            logger.error(f'Bad node index: {index}')
            return None
        return self.nodes[index]

    def set_status_text(self, text):
        self.status_text = text

    def print_status(self, index, request, response):
        status = '%04d: [' % int(time.time() % 10000)
        i = 0
        t = 1
        for node in self.nodes:
            s = node.state.__class__.__name__[0]
            m_in = node.in_queue.qsize()
            m_out = node.out_queue.qsize()
            color = reset
            if s in colormap:
                color = colormap[s]
            if i == index:
                t = node.timeout
                status = status + bunder + color + s + eunder + reset
            else:
                status = status + color + s + reset
            i += 1
        status += f'] {index}:{t} {m_in}:{m_out} {request}'
        if response:
            status += f'=>{response.mdest}: {response.request}'
        if self.status_text:
            status += self.status_text
            self.status_text = None
        print(status)


class Node:
    def __init__(self, control, index=0):
        super().__init__()
        self.control = control
        self.index = index
        self.config = Config(f'node{self.index}.json')
        logger.info(f'config={self.config}')
        self.address = ('localhost', BASEPORT + self.index)
        self.channel = None
        self.timeout = 1
        self.lastTerm = 0
        # The server's state (Follower, Candidate, or Leader).
        self.state = None
        # The server's term number.
        self.currentTerm = 0
        # The candidate the server voted for in its current term, or
        # None if it hasn't voted for any.
        self.votedFor = None
        # A Sequence of log entries. The index into this sequence is the index of the
        # log entry. Unfortunately, the Sequence module defines Head(s) as the entry
        # with index 1, so be careful not to use that!
        self.log = []
        # The index of the latest entry in the log the state machine may apply.
        self.commitIndex = 0
        # The following variables are used only on candidates:
        # The set of servers from which the candidate has received a RequestVote
        # response in its currentTerm.
        self.votesResponded = []
        # The set of servers from which the candidate has received a vote in its
        # currentTerm.
        self.votesGranted = []
        # The following variables are used only on leaders:
        # The next entry to send to each follower.
        self.nextIndex = 0
        # The latest entry that each follower has acknowledged is the same as the
        # leader's. This is used to calculate commitIndex on the leader.
        self.matchIndex = 0

    def __str__(self):
        return f'node{self.index}: ({self.currentTerm}): {self.state}'

    def init(self):
        self.set_state(RestartState)

    def set_state(self, state):
        self.timeout = randint(5, 9)
        logger.debug(f'{self}: Set new state {state}')
        if self.state:
            self.state.leave()
        self.state = state(self)
        self.state.enter()

    def dispatch(self, message):
        logger.debug(f'{self}: Dispatching {message}')
        for index in range(self.control.count):
            if index != self.index:
                m = copy.deepcopy(message)
                m.msource = self.index
                m.mdest = index
                logger.debug(f'{self}: Send {m} to node{index}')
                self.send(m)
                time.sleep(0.1)


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
        if key not in self.config:
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
        self.server_q = Queue()
        self.channel = ServerChannel(self, self.index, self.node.address)

    def qsize(self):
        return self.server_q.qsize()

    def run(self):
        now = time.time()
        while True:
            try:
                message = self.channel.receive()
                # logger.debug(f'{self.node}: Server got message from channel: {message}, now={now}')
            except ChannelTimeout:
                nnow = time.time()
                if nnow - now < self.node.timeout:
                    # logger.debug(f'{self.node}: Not no time for timeout {self.node.timeout}, ({nnow-now})')
                    continue
                # logger.debug(f'{self.node}: Finaly a timeout')
                message = TimeoutMessage(int(time.time()-now))
            except ChannelException:
                message = ExceptionMessage(sys.exc_info())
            self.server_q.put(message)
            nnow = time.time()
            # logger.debug(f'{self.node}: timeout = {self.node.timeout}, time passed: {nnow-now} ({onow-now})')
            now = time.time()

    def receive(self):
        message = self.server_q.get()
        # logger.debug(f'{self.node}: Received {message}')
        return message


class ClientQueue(Thread):
    def __init__(self, node):
        super().__init__()
        self.node = node
        self.index = node.index
        self.client_q = Queue()

    def qsize(self):
        return self.client_q.qsize()

    def run(self):
        logger.info(f'{self.index}: ClientQueue running')
        self.retry = 0
        while True:
            if self.retry > 5:
                logger.error(f'Giving up on sending message: {message}')
                self.retry = 0
            if not self.retry:
                message = self.client_q.get()
            try:
                # logger.debug(f'{self.node}: SendThread: {message}')
                channel = ClientChannel(self.node, self.index, ('localhost', BASEPORT + message.mdest))
                channel.connect()
                channel.send(message)
                self.retry = 0
                # logger.debug(f'{self.node}: Sent {message}')
            except:
                logger.warning(f'Exception sending: {message} {sys.exc_info()[1]}')
                self.retry += 1

    def send(self, message):
        # logger.debug(f'{self.node}: client_q put {message}')
        return self.client_q.put(message)


class Server(Node, Thread):
    def init(self):
        super().init()
        self.in_queue = ServerQueue(self)
        self.out_queue = ClientQueue(self)

    def run(self):
        logger.info(f'node{self.index}: Server ready on {self.address}')
        self.in_queue.start()
        self.out_queue.start()
        while True:
            message = self.in_queue.receive()
            # logger.debug(f'{self.index}: Server received: {message}')
            handle = getattr(self.state, f'on_{message.request}')
            reply = handle(message)
            if reply:
                self.send(reply)
                self.config.save()
            self.control.print_status(index=self.index, request=message,
                                      response=reply)

    def send(self, m):
        if self.index == m.mdest:
            logger.error(f'{self.index}: Sending {m} to myself {m.mdest}')
            return
        self.out_queue.send(m)

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
        print('Starting NodeController')
        nc = NodeController(5)
        nc.init()
        nc.start()
    else:
        client = Client(None)
        client.init(10)
        client.run()
