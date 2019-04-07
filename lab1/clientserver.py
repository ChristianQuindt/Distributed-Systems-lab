import logging
import socket
import json

import constCS
from context import lab_logging

lab_logging.setup()  # init loging channels for the lab


class Server:
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True
    dict1 = {'Luke': "12345672", "Sky": "124576437", 'Walker': "67", 'Peter': "1267"}

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((constCS.HOST, constCS.PORT))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))

    def dictionary(self, name):

        dictData = self.dict1[name]
        return str(dictData)

    def dictionaryALL(self):

        return json.dumps(self.dict1)


    def serve(self):

        self.sock.listen(1)
        while self._serving:  # as long as _serving (checked after connections or socket timeouts)
            try:
                (connection, address) = self.sock.accept()  # returns new socket and address of client
                while True:  # forever
                    data = connection.recv(1024)  # receive data from client
                    data = data.decode('utf-8')
                    subdata = data[0:4]
                    if not data:
                        break  # stop if client stopped
                    if subdata == "GETA":
                        newdata = self.dictionaryALL()


                    else:
                        name = data[4:len(data)]
                        newdata = self.dictionary(name)

                    if not data:
                        break  # stop if client stopped
                    connection.send(newdata.encode('utf-8'))
                connection.close()  # close the connection
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")


class Client:
    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((constCS.HOST, constCS.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def call(self, parameter):
        #parameter = input('Enter your input:')
        self.sock.send(parameter.encode('ascii'))  # send encoded string as data
        self.logger.info("Packets sent")
        data = self.sock.recv(1024)  # receive the response
        self.logger.info("Data received")
        msg_out = data.decode('ascii')
        print(msg_out)  # print the result
        self.logger.info("Client down.")
        return msg_out

    def close(self):
        self.sock.close()
