import sys
import logging
from messages import LogMessage, AckMessage, TimeoutMessage
from channel import RpcClientChannel
from node import BASEPORT

class RaftHandler(logging.Handler):
    def __init__(self, index):
        super().__init__()
        self.index = index
        self.client = RpcClientChannel(self, 0, ('localhost', BASEPORT + self.index))
        self.client.connect()

    def emit(self, record):
        log = LogMessage(self.index, record)
        log.mdest = self.index
        self.client.send(log)
        ack = self.client.receive()
        if isinstance(ack, TimeoutMessage):
            print(f'Timed out waiting for ACK')
        if not isinstance(ack, AckMessage):
            print(f'Got bad ACK: {ack.request}')
        print(f'Saved: {log} {ack}')
        self.index += 1

    def query(self, index):
        return True


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

raftlogger = RaftHandler(int(sys.argv[1]))

logger.addHandler(raftlogger)

logger.debug('DEBUG a log test')
logger.warning('Waarn.....')
logger.error('Error...EEEE')
