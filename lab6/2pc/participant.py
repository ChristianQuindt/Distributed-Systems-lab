import random
import logging
import time

# coordinator messages
from const2PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT, PREPARE_COMMIT
# participant decissions
from const2PC import LOCAL_SUCCESS, LOCAL_ABORT
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT, NEED_DECISION, READY_COMMIT, LOCAL_COMMIT
# misc constants
from const2PC import TIMEOUT

import stablelog


class Participant:
    """
    Implements a two phase commit participant.
    - state written to stable log (but recovery is not considered)
    - in case of coordinator crash, all_participants mutually synchronize states
    - system blocks if all all_participants vote commit and coordinator crashes
    - allows for partially synchronous behavior with fail-noisy crashes
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log(
            "participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.2pc.Participant")
        self.coordinator = {}
        self.all_participants = {}
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not

        return LOCAL_ABORT if random.random() > 2/3 else LOCAL_SUCCESS

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Participant {} entered state {}."
                         .format(self.participant, state))
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self._enter_state('INIT')  # Start in local INIT state.

    def is_Coordinator(self):
        isCoordinator = True
        for p in self.all_participants:
            if p < self.participant:
                isCoordinator = False
        return isCoordinator

    def CoordinatorRole(self, decision):
        self.logger.info("Participant {} is now a coordinator."
                         .format(self.participant))

        self.channel.leave("participant")
        self.all_participants = self.channel.subgroup('participant')

        self.channel.bind(self.channel.join('coordinator'))
        # send own participant decision, so all_participants can anticipate
        self.channel.send_to(self.all_participants, decision)
        # change state to Coordinator state, looking at ur own decision
        # decision can be: LOCAL_ABORT, LOCAL_SUCCESS, PREPARE_COMMIT,
        # GLOBAL_COMMIT, GLOBAL_ABORT

        # Wait results into abort, which is why the outcome,
        # Local_Success or Local_Abort is irrelevant
        if decision in [LOCAL_ABORT, GLOBAL_ABORT, LOCAL_SUCCESS]:
            self._enter_state('WAIT')
        elif decision in [PREPARE_COMMIT]:
            self._enter_state('PRECOMMIT')
        elif decision in [GLOBAL_COMMIT]:
            self._enter_state('COMMIT')

        # Handle the different states
        if self.state == 'WAIT':
            self._enter_state('ABORT')
            self.channel.send_to(self.all_participants, GLOBAL_ABORT)
            return "Coordinator {} terminated in state ABORT." \
                .format(self.participant)
        elif self.state == 'PRECOMMIT':
            self._enter_state('COMMIT')
            self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
            return "Coordinator {} terminated in state COMMIT." \
                .format(self.participant)
        elif self.state == 'COMMIT':
            self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
            return "Coordinator {} terminated in state COMMIT." \
                .format(self.participant)

    def newCoordinator(self):
        newCoordinator = self.participant
        for p in self.all_participants:
            if p < newCoordinator:
                newCoordinator=p
        return newCoordinator

    def run(self):
        # Wait for start of joint commit
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)
        if not msg:  # Crashed coordinator - give up entirely
            # decide to locally abort (before doing anything)
            decision = LOCAL_ABORT

        else:  # Coordinator requested to vote, joint commit starts
            assert msg[1] == VOTE_REQUEST

            # Firstly, come to a local decision
            decision = self._do_work()  # proceed with local activities

            # If local decision is negative,
            # then vote for abort and quit directly
            if decision == LOCAL_ABORT:
                self.channel.send_to(self.coordinator, VOTE_ABORT)

            # If local decision is positive,
            # we are ready to proceed the joint commit
            else:
                assert decision == LOCAL_SUCCESS
                self._enter_state('READY')

                # Notify coordinator about local commit vote
                self.channel.send_to(self.coordinator, VOTE_COMMIT)

                # Wait for coordinator to notify the final outcome
                msg = self.channel.receive_from(self.coordinator, TIMEOUT)

                if not msg:  # Crashed coordinator
                    # Ask all processes for their decisions
                    self.channel.send_to(self.all_participants, NEED_DECISION)
                    if self.is_Coordinator():
                        returnmsg = self.CoordinatorRole(decision)
                        return returnmsg
                    elif decision not in [LOCAL_ABORT, GLOBAL_ABORT]:
                        msg_newC = self.channel.receive_from(self.coordinator, TIMEOUT * 2)
                        if not msg_newC or msg_newC[1] in [LOCAL_ABORT,GLOBAL_ABORT]:
                            self._enter_state('ABORT')
                            decision = GLOBAL_ABORT
                        elif msg_newC[1] == PREPARE_COMMIT:
                            self._enter_state('PRECOMMIT')
                            decision = PREPARE_COMMIT
                    time.sleep(2)
                    msg_newC = self.channel.receive_from(self.channel.subgroup('coordinator'), TIMEOUT * 2)
                    if msg_newC[1] == GLOBAL_ABORT:
                        self._enter_state('ABORT')
                        decision = msg_newC[1]
                    elif msg_newC[1] == GLOBAL_COMMIT:
                        self._enter_state('COMMIT')
                        decision = msg_newC[1]
                    return "Participant {} terminated in state {} due to {}.".format(self.participant, self.state, decision)



                else:  # Coordinator came to a decision
                    decision = msg[1]

        if decision == PREPARE_COMMIT:
            self._enter_state('PRECOMMIT')
            self.channel.send_to(self.coordinator, READY_COMMIT)
        else:
            assert decision in [GLOBAL_ABORT, LOCAL_ABORT]
            self._enter_state('ABORT')

        msg = self.channel.receive_from(self.coordinator, TIMEOUT * 2)
        if not msg:
            if self.is_Coordinator():
                returnmsg = self.CoordinatorRole(decision)
                return returnmsg
            elif decision not in [LOCAL_ABORT, GLOBAL_ABORT]:
                time.sleep(2)
                msg_newC = self.channel.receive_from(self.channel.subgroup('coordinator'), TIMEOUT)
                msg_newC = self.channel.receive_from(self.channel.subgroup('coordinator'), TIMEOUT)
                if msg_newC:
                    self.logger.debug('Message recieved {}'.format(msg_newC[1]))
                    if msg_newC[1] in [GLOBAL_ABORT, LOCAL_ABORT]:
                        self._enter_state('ABORT')
                        decision = GLOBAL_ABORT
                    elif (msg_newC[1] == GLOBAL_COMMIT) and not (self.state == 'ABORT'):
                        self._enter_state('COMMIT')
                        decision = GLOBAL_COMMIT
                else:
                    self._enter_state('ABORT')
                    decision = LOCAL_ABORT
                    self.logger.debug('181')
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, decision)
        else:  # Coordinator came to a decision
            decision = msg[1]

        if decision == GLOBAL_COMMIT:
            self._enter_state('COMMIT')
            self.channel.send_to(self.coordinator, LOCAL_COMMIT)
        else:
            assert decision in [GLOBAL_ABORT, LOCAL_ABORT]
            self._enter_state('ABORT')

        return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
