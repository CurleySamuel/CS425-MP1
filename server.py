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


def send_message(message):
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((TCP_IP, TCP_SENDPORT))
    s2.send(message)
    print bcolors.OKBLUE +  'Sent "' + message + '", system time is ' + \
        str(datetime.datetime.now().time().strftime("%H:%M:%S")) + bcolors.ENDC + \
        bcolors.HEADER +  bcolors.UNDERLINE + "\nEnter Message" + bcolors.ENDC
    print
    s2.close()


def handle_message(msg, port):
    if msg is not None:
        parse = msg.split()
        if parse[1] in ["delete","get","insert","update"]:
            try:
                me = (port == int(parse[0]))
            except Exception:
                me = False
            if parse[1] == "delete":
                try:
                    key_store.pop(parse[2])
                    if me:
                        print "Deleted key " + parse[2]
                except Exception:
                    if me:
                        print "Tried to delete non-existent key " + parse[2]
            elif parse[1] == "get":
                try:
                    if me:
                        print parse[2] + ": " + str(key_store[parse[2]])
                except Exception:
                    if me:
                        print "Failed to get key."
            elif parse[1] == "insert":
                key_store[parse[2]] = parse[3]
                if me:
                    print "Key " + parse[2] + " inserted."
            elif parse[1] == "update":
                if parse[2] in key_store.keys():
                    key_store[parse[2]] = parse[3]
                    if me:
                        print "Key " + parse[2] + " updated."
                else:
                    if me:
                        print "Key " + parse[2] + " does not exist."

        else:
            print bcolors.OKGREEN +  'Received "' + msg + '", system time is ' + \
                str(datetime.datetime.now().time().strftime("%H:%M:%S") + bcolors.ENDC) + \
                bcolors.HEADER +  bcolors.UNDERLINE + "\nEnter Message" + bcolors.ENDC


def listening_thread(listenIP, listenPort, bufferSize):
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((listenIP, listenPort))
    s.listen(1)
    while 1:
        conn, addr = s.accept()
        data = conn.recv(bufferSize)
        handle_message(data, listenPort)
        conn.close()


valid_lengths = {
    "delete": 2,
    "get": 3,
    "insert": 4,
    "update": 4,
    "delay": 2,
    "show-all" : 1
}
def parse_and_validate_message(msg):
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
    return parse


def worker_thread(TCP_IP, TCP_SENDPORT, listen_port, message_queue):
    while 1:
        messages = message_queue.get()
        if messages is not None:
            for message in messages:
                parse = parse_and_validate_message(message)
                if parse is None:
                    continue
                if parse[0].lower() == "delay":
                    try:
                        sleep(float(parse[1]))
                    except ValueError:
                        print "Invalid delay specified."
                elif parse[0].lower() in ["get", "insert", "update"]:
                    # Need to change behavior depending on model. Coding linearizability for now.
                    send_message(" ".join(["bcast",str(listen_port)]+parse))
                elif parse[0].lower() == "delete":
                    send_message(" ".join(["bcast",str(listen_port)]+parse))
                elif parse[0].lower() == "show-all":
                    for key,val in key_store.items():
                        print key + ": " + val
                else:
                    send_message(message)


def main():
    global TCP_IP
    global TCP_SENDPORT
    global key_store
    key_store = {}
    signal.signal(signal.SIGINT, signal_handler)
    TCP_IP = socket.gethostbyname(socket.gethostname())
    TCP_SENDPORT = int(sys.argv[1])
    TCP_RECEIVEPORT = int(sys.argv[2])
    BUFFER_SIZE = 1024
    listener = threading.Thread(target=listening_thread, args=[TCP_IP, TCP_RECEIVEPORT, BUFFER_SIZE])
    listener.daemon = True
    listener.start()
    message_queue = SimpleQueue()
    worker = threading.Thread(target=worker_thread, args=[TCP_IP, TCP_SENDPORT, TCP_RECEIVEPORT, message_queue])
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
