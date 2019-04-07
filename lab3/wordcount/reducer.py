import pickle
import sys


import zmq

import constPipe


name = str(sys.argv[1])

address1 = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT2   # 1st task src
address2 = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT3
address3 = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT4

context = zmq.Context()
subscriber = context.socket(zmq.SUB)

subscriber.connect(address1)
subscriber.connect(address2)
subscriber.connect(address3)


subscriber.setsockopt(zmq.SUBSCRIBE, name.encode())

print("{} started".format(name))

counter = 0
received = {}

while True:
    data = subscriber.recv()
    if data.decode()[2:] in received:
        received[data.decode()[2:]] += 1

    else:
        received.update({data.decode()[2:]: 1})

    for i in received:
        print(i, received[i])
