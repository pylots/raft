import logging
import os
import sys
import time
import json
import copy
from random import randint

from threading import Thread
from queue import Queue, Empty
from channel import Channel, ChannelTimeout, ChannelException
from state import FollowerState
from messages import TimeoutMessage, ExceptionMessage, LogMessage, AckMessage, SystemMessage


BASEPORT = 9000

logging.basicConfig(
    filename='raft.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname).3s %(threadName)s [%(name)s:%(lineno)s] %(message)s',
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
            color = reset
            if s in colormap:
                color = colormap[s]
            if i == index:
                t = node.timeout
                status = status + bunder + color + s + eunder + reset
            else:
                status = status + color + s + reset
            i += 1
        status += f'] {index}:{t} {request}'
        if response:
            status += f'=>{response.mdest}: {response.request}'
        if self.status_text:
            status += self.status_text
            self.status_text = None
        print(status)


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


class Node:
    def __init__(self, control, index=0):
        super().__init__()
        self.control = control
        self.index = index
        self.config = Config(f'node{self.index}.json')
        logger.info(f'config={self.config}')
        self.address = ('localhost', BASEPORT + self.index)
        self.queue = None
        self.nodes = {}
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
        self.queue = ServerQueue(self, self.index, self.address)
        self.queue.start()
        self.set_state(FollowerState)

    def set_state(self, state):
        self.timeout = randint(5, 9)
        logger.debug(f'{self}: Set new state {state}')
        if self.state:
            self.state.leave()
        self.state = state(self)
        self.state.enter()

    def receive(self):
        m = self.queue.receive()
        logger.debug(f'{self}: Received {m}')
        return m

    def send(self, message):
        if self.index == message.mdest:
            logger.error(f'{self}: Sending {message} to myself {message.mdest}')
            return
        if message.mdest not in self.nodes:
            logger.debug(f'{self}: No ClientQueue for {message.mdest}, create it...')
            q = ClientQueue(message.mdest, ('localhost', BASEPORT + message.mdest))
            self.nodes[message.mdest] = q
            q.start()
        node = self.nodes[message.mdest]
        logger.debug(f'{self}: Send {message} to {message.mdest}')
        node.send(message)

    def dispatch(self, message):
        logger.debug(f'{self}: Dispatching {message}')
        for index in range(self.control.count):
            if index != self.index:
                m = copy.deepcopy(message)
                m.msource = self.index
                m.mdest = index
                self.send(m)


class ServerChannel(Thread):
    def __init__(self, node, index, queue, channel):
        super().__init__()
        self.node = node
        self.queue = queue
        self.channel = channel

    def run(self):
        while True:
            try:
                message = self.channel.receive()
                # logger.debug(f'{self.node}: Server got message from channel: {message}, now={now}')
            except ChannelTimeout:
                message = TimeoutMessage(0)
            except ChannelException as e:
                logger.info(f'{self.node}: Read exception {e}, bye...')
                return
            self.queue.put(message)
            # logger.debug(f'{self.node}: timeout = {self.node.timeout}, time passed: {time.time()-now}')


class ServerQueue(Thread):
    def __init__(self, node, index, address):
        super().__init__()
        self.node = node
        self.index = index
        self.address = address
        self.queue = Queue()
        self.connections = {}

    def run(self):
        while True:
            logger.debug(f'{self.index}: Create new Accept channel')
            self.channel = Channel(self.index)
            self.queue.put(SystemMessage('Initialized'))
            running = True
            while running:
                try:
                    channel = self.channel.accept(self.address)
                except ChannelTimeout:
                    self.queue.put(TimeoutMessage(0))
                    continue
                except ChannelException as e:
                    logger.error(f'REINIT: ChannelException {e}')
                    self.queue.put(ExceptionMessage(e))
                    running = False
                    continue
                logger.debug(f'{self.node.index}: Got a new channel: {channel}')
                sc = ServerChannel(self.node, self.index, self.queue, channel)
                self.connections[self.index] = sc
                sc.start()
                self.queue.put(SystemMessage(f'NewClient connected to: {self.address}'))

    def receive(self, timeout):
        try:
            message = self.queue.get(timeout=timeout)
        except Empty:
            message = TimeoutMessage(timeout)
        logger.debug(f'{self.index}: Received message {message} from {message.msource}')
        return message

    def send(self, message):
        self.channel.send(message)


class ClientChannel(Thread):
    def __init__(self, index, channel):
        super().__init__()

    def run(self):
        while True:
            message = self.queue.get()
            self.channel.send(message)


class ClientQueue(Thread):
    def __init__(self, index, address):
        super().__init__()
        self.index = index
        self.address = address
        self.queue = Queue()

    def send(self, message):
        return self.queue.put(message)

    def run(self):
        logger.info(f'{self.index}: ClientQueue running, connect to {self.address}')
        self.retry = 0
        while True:
            self.channel = Channel(self.index)
            try:
                self.channel.connect(self.address)
            except Exception as e:
                logger.error(f'{self.index}: Channel connect exception {e}')
                continue
            logger.debug(f'{self.index}: Connected to {self.address}')
            message = None
            self.retry = 0
            while self.retry < 5:
                if not message:
                    message = self.queue.get()
                try:
                    self.channel.send(message)
                    message = None
                    self.retry = 0
                    continue
                except Exception as e:
                    logger.warning(f'Exception sending: {message} {e} retry={self.retry}')
                    self.retry += 1


class Server(Node, Thread):
    def run(self):
        logger.info(f'node{self.index}: Server ready on {self.address}')
        while True:
            message = self.queue.receive(self.timeout)
            handle = getattr(self.state, f'on_{message.request}')
            reply = handle(message)
            if reply:
                self.send(reply)
                self.config.save()
            self.control.print_status(index=self.index, request=message,
                                      response=reply)

    def close(self):
        self.config.save()


if __name__ == "__main__":
    serve = False
    if 'debug' in sys.argv:
        logger.setLevel(logging.DEBUG)
    print('Starting NodeController')
    nc = NodeController(5)
    nc.init()
    nc.start()
