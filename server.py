#!/usr/bin/env python

import socket
import sys
import threading
import datetime
import signal

def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)
def listeningThread(listenIP, listenPort, bufferSize):

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((listenIP, listenPort))
    #print "\t\t\t\t\t\t\tListening"
    s.listen(1)

    #print '\t\t\t\t\t\t\tConnection Address:', addr
    while 1:
        conn, addr = s.accept()
        data = conn.recv(bufferSize)
        if not data: break
        print '\t\t\t\t\t\t\tReceived "' + data + '", system time is ' + str(datetime.datetime.now().time().strftime("%H:%M:%S"))
        conn.send('OK\n')  # echo
    conn.close()

signal.signal(signal.SIGINT, signal_handler)

TCP_IP = socket.gethostbyname(socket.gethostname())
TCP_SENDPORT = int(sys.argv[1]) 
TCP_RECEIVEPORT = int(sys.argv[2]) 
BUFFER_SIZE = 1024  

listener = threading.Thread(target=listeningThread, args=[TCP_IP, TCP_RECEIVEPORT, BUFFER_SIZE])
listener.start()

while 1:
    message = str(raw_input("Enter Message: \n"))
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((TCP_IP, TCP_SENDPORT))
    s2.send(message)
    data = s2.recv(BUFFER_SIZE)
    print 'Sent "' + message + '", system time is ' + str(datetime.datetime.now().time().strftime("%H:%M:%S"))
    s2.close()
    #print "Returned Message: ", data
