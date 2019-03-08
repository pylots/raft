class Message(object):
    request = 'unknown'

    def __init__(self):
        self.mdest = '*'
        self.msource = '*'

    def __str__(self):
        return f'({self.msource}=>{self.mdest}) {self.request}: '
    

class TimeoutMessage(Message):
    request = 'timeout'

    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def __str__(self):
        return super().__str__() + f'({self.timeout}) '


class ExceptionMessage(Message):
    request = 'exception'
    
    def __init__(self, e):
        super().__init__()
        self.exception = e

    def __str__(self):
        return super().__str__() + f'{self.exception}'


class AckMessage(Message):
    request = 'ack'

    def __init__(self, dst):
        self.mdest = dst
        

class NackMessage(Message):
    request = 'nack'
    
class LogMessage(Message):
    request = 'log'

    def __init__(self, index, record):
        super().__init__()
        self.index = index
        self.record = record
        
    def __str__(self):
        return super().__str__() + f'{self.index}, {self.record}'


class RequestVoteRequest(Message):
    request = 'request_vote_request'
    
    def __init__(self, mterm, mlastLogTerm, mlastLogIndex, msource=None, mdest=None):
        self.mtype = self.request
        self.mterm = mterm
        self.mlastLogTerm = mlastLogTerm
        self.mlastLogIndex = mlastLogIndex
        self.msource = msource
        self.mdest = mdest
        
    def __str__(self):
        return super().__str__() + f' mterm={self.mterm}, mlastLogTerm={self.mlastLogTerm}, mlastLogIndex={self.mlastLogIndex}'
        
class RequestVoteResponse(Message):
    request = 'request_vote_response'

    def __init__(self, mterm, mlastLogTerm, mlastLogIndex, msource=None, mdest=None):
        self.mtype = self.request
        self.mterm = mterm
        self.mlastLogTerm = mlastLogTerm
        self.mlastLogIndex = mlastLogIndex
        self.msource = msource
        self.mdest = mdest
        
    def __str__(self):
        return super().__str__() + f' mterm={self.mterm}, mlastLogTerm={self.mlastLogTerm}, mlastLogIndex={self.mlastLogIndex}'

class AppendEntriesRequest(Message):
    request = 'append_entries_request'

    def __init__(self, mterm, mprevLogIndex, mprevLogTerm, mentries, mcommitIndex, msource=None, mdest=None):
        self.mtype = self.request
        self.mprevLogIndex = mprevLogIndex
        self.mprevLogTerm = mprevLogTerm
        self.mentries = mentries
        self.mcommitIndex = mcommitIndex
        self.msource = msource
        self.mdest = mdest

    def __str(self):
        return super().__str__() + f' mprevLogIndex={self.mprevLogIndex}, mprevLogTerm={self.mprevLogTerm}, mcommitIndex={self.mcommitIndex}'


class AppendEntriesResponse(Message):
    request = 'append_entries_response'

    def __init__(self, mterm, msuccess, mmatchIndex, msource=None, mdest=None, m=None):
        self.mtype = self.request
        self.mterm = mterm
        self.msuccess = msuccess
        self.mmatchIndex = mmatchIndex
        self.msource = msource
        self.mdest = mdest
        self.m = m

    def __str__(self):
        return super().__str__() + f' mterm={self.mterm}, msuccess={self.msuccess}, mmatchIndex={self.mmatchIndex}'
