import logging
from messages import LogMessage, AckMessage
from node import Client


class RaftHandler(logging.Handler):
    def __init__(self, address):
        super().__init__()
        self.client = Client('logger', address)
        self.client.connect('node0')
        self.index = 0
        
    def emit(self, record):
        log = LogMessage(self.index, record)
        self.client.send(log)
        ack = self.client.receive()
        print(f'Saved: {log} {ack}')
        self.index += 1

    def query(self, index):
        return True
    
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

raftlogger = RaftHandler(('localhost', 8888))

logger.addHandler(raftlogger)

logger.debug('Test')
logger.warning('Waarn')
logger.error('Error...')

