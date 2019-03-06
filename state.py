import logging

from messages import *        

logging.basicConfig(filename='raft.log',
                    level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname).3s [%(name)s:%(lineno)s] %(message)s',
                    datefmt='%y%m%d %H%M%S')

logger = logging.getLogger(__name__)

class State(object):
    def __init__(self, node):
        self.node = node
        self.tout = 0
        
    def __str__(self):
        return f'{self.node.index}:{self.__class__.__name__}'

    def enter(self):
        self.tout = 0
        pass

    def leave(self):
        pass
    
    def set_state(self, state):
        logger.debug(f'{self}: new state {state}')
        self.node.state.leave()
        self.node.state = state(self.node)
        self.node.state.enter()

    def exception(self, exc):
        logger.error(f'{self}: exception {exc}')
        return None

    def timeout(self, m):
        logger.info(f'{self}: timeout {m}')
        return None
    
    def log(self, log):
        pass
    
    def request_vote_request(self, vote):
        logger.error(f'{self}: RequestVoteRequest not implemented')

    def request_vote_response(self, response):
        logger.error(f'{self}: RequestVoteResponse not implemented')
        
    def entries(self, entries):
        pass

class RestartState(State):
    def timeout(self, m):
        self.set_state(FollowerState)
        
class FollowerState(State):

    def timeout(self, m):
        self.node.currentTerm += 1
        logger.debug(f'{self}: Timeout, nodes={self.node.control.count}')
        self.node.config['votedFor'] += 1
        for i in range(self.node.control.count):
            index = 4 - i
            if index != self.node.index:
                logger.info(f'{self}: Send RequestVote to node{index}')
                self.node.send(RequestVoteRequest(self.node.currentTerm, 0, 0, self.node.index, index))
        self.set_state(CandidateState)

    def request_vote_request(self, m):
        logger.info(f'{self}: Got VoteRequest from node{m.msource}, term={m.mterm}')
        dest = m.msource
        return RequestVoteResponse(m.mterm, 0, 0, self.node.index, dest)
    
    def request_vote_response(self, m):
        logger.error(f'{self}: Ignore VoteResponse from node{m.msource}, term={m.mterm}')
        return None
    

class CandidateState(State):
    def timeout(self, m):
        logger.debug(f'{self}: Timeout')

    def enter(self):
        self.ntimeout = 0

    def timeout(self, m):
        self.ntimeout += 1
        if self.ntimeout > 3:
            self.set_state(FollowerState)

    def request_vote_request(self, m):
        logger.info(f'{self}: Ignore VoteRequest from node{m.msource}, term={m.mterm}')
        return None
    
    def request_vote_response(self, m):
        logger.info(f'{self}: Yay got VoteResponse from node{m.msource}, term={m.mterm}')
        return None

    
    
class LeaderState(State):
    def timeout(self, m):
        pass

    def log(self, log):
        print(f'log: {log}')
