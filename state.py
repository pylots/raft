from messages import *        


class State(object):
    def __init__(self, node):
        self.node = node
        self.tout = 0
        
    def enter(self):
        self.tout = 0
        pass

    def leave(self):
        pass
    
    def set_state(self, state):
        self.node.state.leave()
        self.node.state = state(self.node)
        self.node.state.enter()

    def exception(self, exc):
        print(f'{self}: exception {exc}')
        return None

    def timeout(self, m):
        print(f'{self}: timeout {m}')
        return None
    
    def log(self, log):
        pass
    
    def vote(self, vote):
        pass

    def entries(self, entries):
        pass
    
    def __str__(self):
        return self.__class__.__name__

class RestartState(State):
    def timeout(self, m):
        print(f'state{self.node.index}: timeout in restart, nodes={self.node.control.count}')
        self.node.config['votedFor'] += 1
        for index in range(self.node.control.count):
            if index != self.node.index:
                print(f'state{self.node.index}: Send RequestVote to {index}')
                self.node.send(RequestVoteMessage(0, 0, 0, self.node.index, index))

    def vote(self, m):
        print(f'state{self.node.index}: Got a vote request from {m.msource}')
        dest = m.msource
        return RequestVoteMessageResponse(0, 0, 0, self.node.index, dest)
    
    def vote_response(self, m):
        print(f'state{self.node.index}: Got a vote response from {m.msource}')
        return None
    
class FollowerState(State):
    def timeout(self, m):
        self.dispatch.requestVote()
        self.set_state(CandidateState)

class CandidateState(State):
    def timeout(self, m):
        pass
    
class LeaderState(State):
    def timeout(self, m):
        pass

    def log(self, log):
        print(f'log: {log}')
