class Message(object):
    request = 'unknown'

    def __init__(self):
        self.mdest = None
        self.msource = None

    def __str__(self):
        return f'dest={self.mdest}, req={self.request}: '
    

class TimeoutMessage(Message):
    request = 'timeout'

    def __str__(self):
        return super().__str__() + f'timeout'


class ExceptionMessage(Message):
    request = 'exception'
    
    def __init__(self, e):
        super().__init__()
        self.exception = e

    def __str__(self):
        return super().__str__() + f'exception {self.exception}'


class AckMessage(Message):
    request = 'ack'

    def __init__(self, dst):
        self.mdest = dst
        
    def __str__(self):
        return super().__str__() + f'ACK'
    

class NackMessage(Message):
    request = 'nack'
    
    def __str__(self):
        return super.__str__() + f'NACK'
    

class LogMessage(Message):
    request = 'log'

    def __init__(self, index, record):
        super().__init__()
        self.index = index
        self.record = record
        
    def __str__(self):
        return super().__str__() + f'LogMessage: {self.index}, {self.record}'


class RequestVoteRequest(Message):
    request = 'request_vote_request'
    
    def __init__(self, mterm, mlastLogTerm, mlastLogIndex, msource, mdest):
        self.mtype = self.request
        self.mterm = mterm
        self.mlastLogTerm = mlastLogTerm
        self.mlastLogIndex = mlastLogIndex
        self.msource = msource
        self.mdest = mdest
        
    def __str__(self):
        return super().__str__() + f'Vote'

class RequestVoteResponse(Message):
    request = 'request_vote_response'

    def __init__(self, mterm, mlastLogTerm, mlastLogIndex, msource, mdest):
        self.mtype = self.request
        self.mterm = mterm
        self.mlastLogTerm = mlastLogTerm
        self.mlastLogIndex = mlastLogIndex
        self.msource = msource
        self.mdest = mdest
        
    def __str__(self):
        return super().__str__() + f'VoteResponse (to={self.mdest}, from={self.msource})'

