#!/usr/bin/env python

import socket
import sys
import threading
import datetime
import signal

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

def signal_handler(signal, frame):

    print(bcolors.FAIL + bcolors.BOLD + 'Force Quit' + bcolors.ENDC)
    exit(0)

def readFile(fileName):
    
    with open(fileName) as f:
        commands = f.read().splitlines()
    return commands

def listeningThread(listenIP, listenPort, bufferSize):

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((listenIP, listenPort))
    s.listen(1)

    while 1:
        conn, addr = s.accept()
        data = conn.recv(bufferSize)
        if not data: break
        print bcolors.OKGREEN + bcolors.BOLD + 'Received "' + data + '", system time is ' + \
            str(datetime.datetime.now().time().strftime("%H:%M:%S") + bcolors.ENDC) + \
            bcolors.HEADER + bcolors.BOLD + bcolors.UNDERLINE + "\nEnter Message" + bcolors.ENDC
        #conn.send('ACK')   
    conn.close()



signal.signal(signal.SIGINT, signal_handler)

TCP_IP = socket.gethostbyname(socket.gethostname())
TCP_SENDPORT = int(sys.argv[1])
TCP_RECEIVEPORT = int(sys.argv[2])
BUFFER_SIZE = 1024

listener = threading.Thread(target=listeningThread, args=[TCP_IP, TCP_RECEIVEPORT, BUFFER_SIZE])
listener.daemon = True
listener.start()

while 1:
    command = str(raw_input(bcolors.HEADER + bcolors.BOLD + bcolors.UNDERLINE + "Enter Message:\n" + bcolors.ENDC))
    messages = []

    if '.txt' in command:
        messages = readFile(command)
    else:
        messages.append(command)

    for message in messages:
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.connect((TCP_IP, TCP_SENDPORT))
        s2.send(message)
        #data = s2.recv(BUFFER_SIZE) #Recieve ACK
        print bcolors.OKBLUE + bcolors.BOLD + 'Sent "' + message + '", system time is ' + \
            str(datetime.datetime.now().time().strftime("%H:%M:%S")) + bcolors.ENDC
        s2.close()
