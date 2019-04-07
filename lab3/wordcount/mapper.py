import pickle
import sys


import zmq

import constPipe

port = str(sys.argv[1])

address1_pull = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT1  # 1st task src
context = zmq.Context()
pull_socket_splitter = context.socket(zmq.PULL)  # create a pull socket
pull_socket_splitter.connect(address1_pull)  # connect to task source 1

publisher = context.socket(zmq.PUB)

address = "tcp://" + constPipe.SRC1 + ":" + port

publisher.bind(address)

print("{} started".format(port))

while True:
    print("waiting for work")
    work = pickle.loads(pull_socket_splitter.recv())  # receive work from a source
    print("work received")
    woerter = work[1].split(" ")

    for value in woerter:
        send = '{}'.format((len(value) % 2)) + " " + value
        print(send)
        publisher.send(send.encode())
        print(value)