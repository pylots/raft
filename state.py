from events import *        


class State(object):
    def __init__(self, server):
        self.server = server
        self.tout = 0
        
    def enter(self):
        self.tout = 0
        pass

    def leave(self):
        pass
    
    def set_state(self, state):
        self.server.state.leave()
        self.server.state = state(self.server)
        self.server.state.enter()

    def exception(self, exc):
        print(f'{self}: exception {exc}')
        return None

    def timeout(self, m):
        print(f'{self}: timeout {m}')
        self.tout += 1
        if self.tout > 5:
            self.set_state(TimeoutState)
        return None
    
    def log(self, log):
        print(f'{log}')
        return AckEvent(log.seq)
    
    def vote(self, vote):
        print(f'{vote}')
        return AckEvent(vote.seq)

    def entries(self, entries):
        print(f'{entries}')
        return AckEvent(entries.seq)
    
    def __str__(self):
        return self.__class__.__name__


class TimeoutState(State):
    def enter(self):
        print(f'Enter Timeout')

    def timeout(self, m):
        print(f'Going back to State')
        self.set_state(State)
        

class FollowerState(State):
    def timeout(self):
        self.set_state(CandidateState)

class CandidateState(State):
    def timeout(self):
        pass
    
class LeaderState(State):
    def timeout(self):
        self.set_state(VoteState)

    def log(self, log):
        print(f'log: {log}')
