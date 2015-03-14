#!/usr/bin/env python

import socket
import sys
import threading
import datetime
import signal
import atexit
import os
from multiprocessing.queues import SimpleQueue
from time import sleep

"""   START OF MESSAGE CLASS   """

valid_keywords = ["delete", "get", "insert", "update", "ack", "return"]
class Message:
    socket = None
    keyword = None
    key = None
    val = None
    model = None
    outbound = None
    msg = None
    dst = None
    tstamp = None
    __validated = False
    v_error = None
    me = False
    def __init__(self, outbound, msg):
        assert outbound in [True, False]
        if msg:
            if not outbound:
                # This is an inbound message.
                self.outbound = False
                parse = msg.split()
                try:
                    # SEND/BCAST need to be handled separately
                    if len(parse) < 3 or parse[1].lower() not in valid_keywords:
                        self.keyword = "send"
                        self.msg = " ".join(parse)
                    else:
                        self.tstamp = parse[-1]
                        # Every message will contain a socket, keyword.
                        self.socket = int(parse[0])
                        if self.socket == TCP_RECEIVE_PORT:
                            self.me = True
                        self.keyword = parse[1].lower()
                        self.key = parse[2]

                        # But get will not contain a value
                        if self.keyword == "get":
                            self.model = int(parse[3])
                        elif self.keyword in ["insert", "update", "return"]:
                            self.val = (parse[3],parse[5])
                            self.model = int(parse[4])

                except Exception as e:
                    print "Error parsing inbound message: ", e
                    return
            else:
                # This is an outbound message.
                self.socket = TCP_RECEIVE_PORT
                currentTime = datetime.datetime.now().time().strftime("%H:%M:%S") #TODO: 
                self.outbound = True
                parse = msg.split()
                try:
                    # Every message will contain a keyword.
                    self.keyword = parse[0].lower()

                    # Need to handle send/bcast separately.
                    if self.keyword in ["send", "bcast"]:
                        self.msg = " ".join(parse[1:-1])
                        self.dst = parse[-1]
                    else:
                        self.key = parse[1]
                        # Need to handle get differently
                        if self.keyword == "get":
                            self.model = int(parse[2])
                            self.val = (None,currentTime) #Time of get requst, value is still blank
                        else:
                            self.val = (parse[2],currentTime)
                            self.model = int(parse[3])

                except Exception, e:
                    print "Error parsing outbound message: ", e

        else:
            # No msg provided.
            if outbound:
                self.socket = TCP_RECEIVE_PORT


    # Will attempt to validate. If false is returned then error is stored in v_error.
    def validate(self):

        #TODO: Bypass validation, fix later
        if self.keyword in ["ack","return"]:
            return True

        # Keyword
        if self.keyword not in valid_keywords:
            self.v_error = "Invalid keyword"
            return False

        # Validate Send/BCAST
        if self.keyword in ["send", "bcast"]:
            if self.msg is None or ((self.dst is None and self.keyword != "bcast") and self.outbound is True):
                self.v_error = "Malformed SEND/BCAST request"
                return False
            self.__validated = True
            return True

        # Key
        if self.key is None or (self.key not in key_store.keys() and self.keyword != "insert"):
            self.v_error = "Key does not exist"
            return False

        # Model
        if (self.model is None and self.keyword != "delete") or self.model not in range(1,5):
            self.v_error = "Invalid consistency model"
            return False

        # tstamp
        """
        if self.tstamp is None:
            self.v_error = "Invalid timestamp"
            return False
        """

        self.__validated = True
        return True


    # Will convert the Message object into a string that should be sent to the network.
    def to_message(self):
        if self.keyword in ["bcast", "send"]:
            return " ".join([self.keyword, self.msg, self.dst or ""])
        return " ".join(["bcast", str(self.socket), self.keyword, self.key, self.val[0] or "", str(self.model or ""), self.val[1]])


    # If message isn't already validated send will attempt to validate.
    # If validation fails then send will throw an assertion error.
    def send(self):
        if not self.__validated:
            self.validate()
            assert self.__validated == True
        message = self.to_message()
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.connect((TCP_SEND_IP, TCP_SEND_PORT))
        s2.send(message)
        print bcolors.OKBLUE +  'Sent "' + message + '", system time is ' + \
            str(datetime.datetime.now().time().strftime("%H:%M:%S")) + bcolors.ENDC + \
            bcolors.HEADER +  bcolors.UNDERLINE + "\nEnter Message" + bcolors.ENDC
        s2.close()

    def duplicate(self):
        copy = Message(self.outbound, None)
        copy.socket = self.socket
        copy.keyword = self.keyword
        copy.key = self.key
        copy.val = self.val
        copy.model = self.model
        copy.outbound = self.outbound
        copy.msg = self.msg
        copy.dst = self.dst
        copy.tstamp = self.tstamp
        copy.__validated = self.__validated
        copy.v_error = self.v_error
        copy.me = self.me
        return copy

    def create_ack(self):
        ack = self.duplicate()
        ack.outbound = True
        ack.keyword = "ack"
        return ack

    def create_return(self, cur_value):
        return_message = self.duplicate()
        return_message.outbound = True
        return_message.keyword = "return"
        return_message.val = cur_value
        return return_message

"""    END OF MESSAGE CLASS    """

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


def signal_handler(signal, frame):
    s.close()
    exit(0)


def readFile(fileName):
    try:
        with open(fileName) as f:
            commands = f.read().splitlines()
        return commands
    except IOError:
        print bcolors.FAIL + "File does not exist" + bcolors.ENDC
        return None


def handle_message(msg):
    print "handling message - " + msg
    msg = Message(False, msg)
    if not msg.validate():
        print "Inbound message failed validation: ", msg.v_error
        return
    if msg.keyword == "delete":
        key_store.pop(msg.key)

    elif msg.keyword == "get":
        if (msg.me and msg.model in range(1,2)) or msg.model in range(3,4):
            return_message = msg.create_return(key_store[msg.key])
            return_message.send()  
            #print msg.key + ": " + str(key_store[msg.key])

    elif msg.keyword == "insert":
        key_store[msg.key] = msg.val
        if msg.me:
            ack = msg.create_ack()
            ack.send()
    elif msg.keyword == "update":
        key_store[msg.key] = msg.val
        if msg.me:
            ack = msg.create_ack()
            ack.send()
    elif msg.keyword == "send":
        print bcolors.OKGREEN +  'Received "' + msg.msg + '", system time is ' + \
            str(datetime.datetime.now().time().strftime("%H:%M:%S") + bcolors.ENDC) + \
            bcolors.HEADER +  bcolors.UNDERLINE + "\nEnter Message" + bcolors.ENDC
    elif msg.keyword == "ack":
        if msg.me:
            print "ACK Received"

    elif msg.keyword == "return":
        if msg.me:
            if msg.model in range(1,2):
                #Linearizable or Sequential Consistency
                print "Returned " + str(msg.key) + " - " + str(msg.val)
            else:
                #Eventual Consistency
                requestID = msg.socket + "-" + str(msg.val[1]) 
                if requestID in eventual_requests: #TODO: Conditional might be unneccesary - entry in dictionary created upon outbound message
                    #Check if entry for get value is the latest, increment get counter
                    currentLatestTime = eventual_requests[requestID][1]
                    if currentLatestTime - msg.val[1] > 0:
                        eventual_requests[requestID][0] = msg.val[0]
                        eventual_requests[requestID][1] = msg.val[1]
                    eventual_requests[requestID][2] += 1

                    #If get counter reachers appropriate R for model, return result and remove entry from dictionary
                    if eventual_requests[requestID][2] == (msg.model-2):
                        latestValue = (eventual_requests[requestID][0], eventual_requests[requestID][1])
                        eventual_requests.pop(requestID)
                        print "Returned " + str(msg.key) + " - " + str(latestValue)
                         


def listening_thread(bufferSize):
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_RECEIVE_IP, TCP_RECEIVE_PORT))
    s.listen(1)
    while 1:
        conn, addr = s.accept()
        data = conn.recv(bufferSize)
        handle_message(data)
        conn.close()


valid_lengths = {
    "delete": 2,
    "get": 3,
    "insert": 4,
    "update": 4,
    "delay": 2,
    "show-all" : 1
}
def parse_and_validate_command(msg):
    if msg is None:
        return None
    parse = msg.split()
    if parse[0].lower() in valid_lengths.keys():
        if len(parse) != valid_lengths[parse[0].lower()]:
            print "Invalid command. Expected " + str(valid_lengths[parse[0].lower()]-1) + " arguments, got " + str(len(parse)-1)
            return None
    else:
        if parse[0].lower() in ["send", "bcast"]:
            if len(parse) < 2:
                print "Invalid command. Missing message."
                return None
        else:
            print "Unrecognized command."
            return None
    if parse[0].lower() in ["get", "update", "insert"]:
        try:
            consist = int(parse[-1])
            if consist not in range(1,5):
                raise ValueError
        except Exception:
            print "Not a valid consistency model."
            return None
    return parse


def worker_thread(message_queue):
    while 1:
        messages = message_queue.get()
        if messages is not None:
            for message in messages:
                parse = parse_and_validate_command(message)
                if parse is None:
                    continue
                if parse[0].lower() == "delay":
                    try:
                        sleep(float(parse[1]))
                    except ValueError:
                        print "Invalid delay specified."
                elif parse[0].lower() == "show-all":
                    for key,val in key_store.items():
                        print key + ": " + val[0]
                else:
                    # Create the message object
                    msg = Message(True, message)
                    # Validate the message object
                    if not msg.validate():
                        print "Invalid command:", msg.v_error
                        continue
                    
                    # Consistency of 2 for get means we don't need to send the message
                    if msg.keyword == "get" and msg.model == 2:
                        print msg.key + ": " + str(key_store[msg.key])
                    else:
                         #Eventual Consistency Requests
                        requestID = msg.socket + "-" + msg.val[1]
                        if msg.model in range(3,4):
                            if msg.keyword == "get":
                                eventual_requests[requestID] = (None,None,0) #If get, need to store number of requests and latest value & timestamp
                            elif msg.keyword in ["insert","update"]:
                                eventual_requests[requestID] = 0 #Only need to store number of acks recieved
                        msg.send()



def main():
    global TCP_SEND_PORT
    global TCP_SEND_IP
    global TCP_RECEIVE_IP
    global TCP_RECEIVE_PORT
    global key_store
    global eventual_requests
    key_store = {}
    signal.signal(signal.SIGINT, signal_handler)
    TCP_RECEIVE_IP = TCP_SEND_IP = '10.0.0.6'#socket.gethostbyname(socket.gethostname())
    TCP_SEND_PORT = int(sys.argv[1])
    TCP_RECEIVE_PORT = int(sys.argv[2])
    BUFFER_SIZE = 1024
    listener = threading.Thread(target=listening_thread, args=[BUFFER_SIZE])
    listener.daemon = True
    listener.start()
    message_queue = SimpleQueue()
    worker = threading.Thread(target=worker_thread, args=[message_queue])
    worker.daemon = True
    worker.start()

    while 1:
        command = str(raw_input(bcolors.HEADER +  bcolors.UNDERLINE + "Enter Message:\n" + bcolors.ENDC))
        messages = []
        if command.endswith('.txt'):
            messages = readFile(command)
        else:
            messages.append(command)
        message_queue.put(messages)

if __name__ == "__main__":
    main()
