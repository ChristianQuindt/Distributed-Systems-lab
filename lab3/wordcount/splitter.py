import pickle
import random
import sys
import time

import zmq

import constPipe


context = zmq.Context()
push_socket = context.socket(zmq.PUSH)  # create a push socket

address = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT1  # how and where to connect
push_socket.bind(address)  # bind socket to address

time.sleep(1) # wait to allow all clients to connect

for i in range(1):  # generate 10 workloads
    print("wordload erstellt")
    file = open("text", 'r')
    satz = file.read()

    push_socket.send(pickle.dumps((1, satz)))  # send workload to worker
    print("pushed sentence")
