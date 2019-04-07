import logging
import random
import time
from threading import Thread, Lock
from multiprocessing import Process, Value, Array

from constMutex import ENTER, RELEASE, ALLOW


class Process:
    """
    Implements access management to a critical section (CS) via fully
    distributed mutual exclusion (MUTEX).

    Processes broadcast messages (ENTER, ALLOW, RELEASE) timestamped with
    logical (lamport) clocks. All messages are stored in local queues sorted by
    logical clock time.

    A process broadcasts an ENTER request if it wants to enter the CS. A process
    that doesn't want to ENTER replies with an ALLOW broadcast. A process that
    wants to ENTER and receives another ENTER request replies with an ALLOW
    broadcast (which is then later intime than its own ENTER request).

    A process enters the CS if a) its ENTER message is first in the queue (it is
    the oldest pending message) AND b) all other processes have send messages
    that are younger (either ENTER or ALLOW). Release requests purge
    corresponding ENTER requests from the top of the local queues.

    Message Format:

    <Message>: (Timestamp, Process_ID, <Request_Type>)

    <Request Type>: ENTER | ALLOW  | RELEASE

    """

    def __init__(self, chan):
        self.channel = chan  # Create ref to actual channel
        self.process_id = self.channel.join('proc')  # Find out who you are
        self.all_processes: list = []  # All procs in the proc group
        self.living_processes: list = []
        self.other_processes: list = []  # Needed to multicast to others
        self.queue = []  # The request queue list
        self.clock = 0  # The current logical clock
        self.logger = logging.getLogger("vs2lab.lab5.mutex.process.Process")
        self.mutex = Lock()

    def addToLiving(self, x):
        if not self.living_processes.__contains__(x):
            self.living_processes.append(x)
            self.logger.debug("Initial allprocesses")
            self.logger.debug(self.all_processes)
            self.logger.debug("living")
        self.logger.debug(self.living_processes)


    def addToLiving2(self):
        self.all_processes = list(self.channel.subgroup('proc'))

    def check(self, x):
        self.clock = self.clock + 1  # Increment clock value

        for i in self.all_processes:
            alive = False
            for x in self.living_processes:
                if (i == x):
                    alive = True
            if (not alive):
                self.all_processes.remove(i)
                self.other_processes.remove(i)
                self.logger.debug("fixed allprocesses")
                self.logger.debug(self.all_processes)
                self.logger.debug("i fIXed DiS11")
        self.living_processes = self.all_processes


#1. kommt zum timeout, addtoliving wird für jeden prozess ausgeführt. jeder prozess trägt sich in die liste ein. der prozess, der in der alten liste (all_processes & other_processes) steht wird rausgeschmissen
    #problem: living_processes nicht global?

    def __mapid(self, id='-1'):
        # resolve channel member address to a human friendly identifier
        if id == '-1':
            id = self.process_id
        return 'Proc_' + chr(65 + self.all_processes.index(id))

    def __cleanup_queue(self):
        if len(self.queue) > 0:
            self.queue.sort()
            # There should never be old ALLOW messages at the head of the queue
            while self.queue[0][2] == ALLOW:
                del (self.queue[0])
                if len(self.queue) == 0:
                    break

    def __request_to_enter(self):
        self.clock = self.clock + 1  # Increment clock value
        request_msg = (self.clock, self.process_id, ENTER)
        self.queue.append(request_msg)  # Append request to queue
        self.__cleanup_queue()  # Sort the queue
        self.channel.send_to(self.other_processes, request_msg)  # Send request

    def __allow_to_enter(self, requester):
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, ALLOW)
        self.channel.send_to([requester], msg)  # Permit other

    def __release(self):
        # need to be first in queue to issue a release
        assert self.queue[0][1] == self.process_id, 'State error: inconsistent local RELEASE'

        # construct new queue from later ENTER requests (removing all ALLOWS)
        tmp = [r for r in self.queue[1:] if r[2] == ENTER]
        self.queue = tmp  # and copy to new queue
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, RELEASE)
        # Mulicast release notofication
        self.channel.send_to(self.other_processes, msg)

    def __allowed_to_enter(self):
         # See who has sent a message
        processes_with_later_message = set([req[1] for req in self.queue[1:]])
        # Access granted if this process is first in queue and all others have answered (logically) later
        first_in_queue = self.queue[0][1] == self.process_id
        all_have_answered = len(self.other_processes) == len(processes_with_later_message)
        return first_in_queue and all_have_answered

    def __receive(self):
         # Pick up any message
        _receive = self.channel.receive_from(self.other_processes, 3)

        if _receive:
            msg = _receive[1]

            self.clock = max(self.clock, msg[0])  # Adjust clock value...
            self.clock = self.clock + 1  # ...and increment

            self.logger.debug("{} received {} from {}.".format(
                self.__mapid(),
                "ENTER" if msg[2] == ENTER
                else "ALLOW" if msg[2] == ALLOW
                else "RELEASE", self.__mapid(msg[1])))

            if msg[2] == ENTER:
                self.queue.append(msg)  # Append an ENTER request
                # and unconditionally allow (don't want to access CS oneself)
                self.__allow_to_enter(msg[1])
            elif msg[2] == ALLOW:
                self.queue.append(msg)  # Append an ALLOW
            elif msg[2] == RELEASE:
                # assure release requester indeed has access (his ENTER is first in queue)
                assert self.queue[0][1] == msg[1] and self.queue[0][2] == ENTER, 'State error: inconsistent remote RELEASE'
                del (self.queue[0])  # Just remove first message

            self.__cleanup_queue()  # Finally sort and cleanup the queue
        else:

            self.clock = self.clock + 1  # ...and increment
            #self.addToLiving(self.process_id)
            #self.addToLiving2()
            self.all_processes = list(self.channel.subgroup('proc'))

            asd = []
            asd.append(self.all_processes)
            self.other_processes = asd.remove(self.process_id)
            time.sleep(2)
            if (len(self.living_processes) >= (len(self.all_processes)-1)):
                self.check(self.process_id)

            self.logger.warning("{} timed out on RECEIVE.".format(self.__mapid()))

    def init(self):
        self.channel.bind(self.process_id)

        self.all_processes = list(self.channel.subgroup('proc'))
        self.all_processes_count = len(self.all_processes)
        # sort string elements by numerical order
        self.all_processes.sort(key=lambda x: int(x))

        self.other_processes = list(self.channel.subgroup('proc'))
        self.other_processes.remove(self.process_id)

        self.logger.info("Member {} joined channel as {}."
                         .format(self.process_id, self.__mapid()))

    def run(self):
        while True:
            # Enter the critical section if there are more than one processes left
            # and random is true
            if len(self.all_processes) > 1 and \
                    random.choice([True, False]):
                self.logger.debug("{} wants to ENTER CS at CLOCK {}."
                    .format(self.__mapid(), self.clock))

                self.__request_to_enter()
                while not self.__allowed_to_enter():
                    self.__receive()

                # Stay in CS for some time ...
                sleep_time = random.randint(0, 2000)
                self.logger.debug("{} enters CS for {} milliseconds."
                    .format(self.__mapid(), sleep_time))
                print(" CS <- {}".format(self.__mapid()))
                time.sleep(sleep_time/1000)

                # ... then leave CS
                print(" CS -> {}".format(self.__mapid()))
                self.__release()
                continue

            # Occasionally serve requests to enter (
            if random.choice([True, False]):
                self.__receive()
