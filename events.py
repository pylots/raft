class Event(object):
    request = 'unknown'
    seq = 0

    def __str__(self):
        return f'({self.seq}) {self.request}: '
    

class TimeoutEvent(Event):
    request = 'timeout'
    
    def __str__(self):
        return super().__str__() + f'timeout'


class ExceptionEvent(Event):
    request = 'exception'
    
    def __init__(self, e):
        self.exception = e

    def __str__(self):
        return super().__str__() + f'exception {self.exception}'


class AckEvent(Event):
    request = 'ack'
    
    def __init__(self, seq):
        self.seq = seq

    def __str__(self):
        return super().__str__() + f'ACK'
    

class NackEvent(Event):
    request = 'nack'
    
    def __init__(self, seq):
        self.seq = seq
        
    def __str__(self):
        return super.__str__() + f'NACK'
    

class LogEvent(Event):
    request = 'log'

    def __init__(self, index, record):
        self.index = index
        self.record = record
        
    def __str__(self):
        return super().__str__() + f'LogEvent: {self.index}, {self.record}'


class RequestVoteEvent(Event):
    request = 'vote'
    
    def __str__(self):
        return super().__str__() + f'Vote'
