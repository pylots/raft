import logging
from events import LogEvent, AckEvent
from node import Client

class RaftHandler(logging.Handler):
    def __init__(self, address):
        super().__init__()
        self.client = Client(address)
        self.client.connect()
        self.index = 0
        
    def emit(self, record):
        log = LogEvent(self.index, record)
        if not self.client.call(log):
            print(f'FAILED: {log}')
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

