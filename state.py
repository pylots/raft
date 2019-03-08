import logging

from random import randint

from messages import *        

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

    def get_node(self, index):
        return self.node.control.get_node(index)

    def on_exception(self, exc):
        logger.error(f'{self}: exception {exc}')
        return None

    def on_timeout(self, m):
        logger.info(f'{self}: timeout {m}')
        return None

    def on_request_vote_request(self, vote):
        logger.error(f'{self}: RequestVoteRequest not implemented')

    def on_request_vote_response(self, response):
        logger.error(f'{self}: RequestVoteResponse not implemented')

    def on_append_entries_request(self, entries):
        logger.error(f'{self}: AppendEntriesRequest not implemented')

    def on_append_entries_response(self):
        logger.error(f'{self}: AppendEntriesResponse not implemented')


class RestartState(State):
    def on_timeout(self, m):
        self.node.currentTerm = 1  # 142
        self.node.votedFor = None
        self.node.set_state(FollowerState)
        self.node.votesResponded = []

    def on_request_vote_request(self, vote):
        self.on_timeout(self, None)

    def on_append_entries_request(self, entries):
        self.on_timeout(self, None)


class FollowerState(State):
    def enter(self):
        self.node.timeout = randint(3, 9)

    def on_timeout(self, m):
        logger.debug(f'{self}: Got timeout in Followerstate, going to Candidate')
        self.node.set_state(CandidateState)

    def on_request_vote_request(self, m):
        snode = self.get_node(m.msource)
        if m.mterm > self.node.currentTerm:
            logger.debug(f'{self}: New term {m.mterm} from {self.node.currentTerm}')
            self.node.votedFor = snode
            self.node.currentTerm = m.mterm
        elif self.node.votedFor:
            logger.info(f'{self}: Already voted for {self.node.votedFor} in term {self.node.currentTerm}')
            return
        logger.info(f'{self}: Got VoteRequest from {snode}, term={m.mterm}')
        self.node.logOk = m.mlastLogTerm > self.node.lastTerm or (m.mlastLogTerm == self.node.lastTerm and m.mlastLogIndex >= len(self.node.log))
        dest = m.msource
        self.node.votedFor = snode
        return RequestVoteResponse(m.mterm, 0, 0, self.node.index, dest)

    def on_request_vote_response(self, m):
        logger.error(f'{self}: Ignore VoteResponse in from node{m.msource}, term={m.mterm}')

    def on_append_entries_request(self, m):
        logger.debug(f'{self}: Append {m} term={m.mterm}, current={self.node.currentTerm}')
        self.votedFor = None
        if m.mterm > self.node.currentTerm:
            logger.warning(f'{self}: Lost messages')
            return AppendEntriesResponse(self.node.currentTerm, False, 0, self.node.index, m.msource, None)
        return AppendEntriesResponse(self.node.currentTerm, True, 0, self.node.index, m.msource, None)


class CandidateState(State):
    def enter(self):
        self.node.currentTerm += 1
        self.node.votedFor = None
        logger.debug(f'{self}: Enter CandidateState')
        # Vote for myself first
        self.node.votesResponded = []
        self.node.votesResponded.append(self.node.index)
        self.node.votesGranted = []
        self.node.voterLog = []
        self.node.dispatch(RequestVoteRequest(self.node.currentTerm, 0, 0))
        self.node.timeout = 10

    def on_timeout(self, m):
        logger.debug(f'{self}: Got timeout in CandidateState, goto FollowerState')
        self.node.set_state(FollowerState)

    def on_request_vote_request(self, m):
        if m.mterm > self.node.currentTerm:
            logger.info('Someone further ahead than me')
            self.node.set_state(FollowerState)
            self.node.currentTerm = m.mterm
            self.votedFor = self.get_node(m.msource)
            return RequestVoteResponse(m.mterm, 0, 0, self.node.index, m.msource)
        logger.info(f'{self}: Ignore VoteRequest from node{m.msource}, term={m.mterm}')
        return None

    def on_request_vote_response(self, m):
        snode = self.get_node(m.msource)
        logger.info(f'{self}: Yay got {m}, term={m.mterm}, voting for me {len(self.node.votesResponded)}')
        if m.mterm > self.node.currentTerm:
            t = f'Someone is ahead of me...{m.mterm} > {self.node.currentTerm}'
            logger.info(t)
            self.node.set_status_text(t)
            self.node.currentTerm = m.mterm
            self.node.set_state(FollowerState)
            self.node.votedFor = snode
            return RequestVoteResponse(m.mterm, 0, 0, self.node.index, m.msource)
        if m.msource in self.node.votesResponded:
            logger.error(f'Already have a vote from {m.msource}')
        self.node.votesResponded.append(snode)
        if len(self.node.votesResponded) >= self.node.control.count / 2 + 1:
            logger.info(f'{self}: We have a new leader, me!')
            self.node.set_state(LeaderState)

    def on_append_entries_request(self, m):
        logger.debug(f'{self}: Got a {m}')
        self.node.set_state(FollowerState)
        return AppendEntriesResponse(self.node.currentTerm, True, 0, self.node.index, m.msource, None)

class LeaderState(State):
    def enter(self):
        self.node.timeout = 1

    def on_timeout(self, m):
        self.node.dispatch(AppendEntriesRequest(self.node.currentTerm, 0, 0, [], 0))

    def on_append_entries_request(self, m):
        logger.debug(f'{self}: Got Append in Leader {m}')
        if m.mterm > self.node.currentTerm:
            logger.warning(f'{self}: Another Leader is higher than me, going to Follow')
            self.node.currentTerm = m.mterm
            self.node.set_state(FollowerState)
            return AppendEntriesResponse(self.node.currentTerm, True, 0, self.node.index, m.msource, None)
        logger.debug(f'Ignoring Append message...')
        return None

    def on_append_entries_response(self, m):
        logger.debug(f'{self}: Got {m}')
