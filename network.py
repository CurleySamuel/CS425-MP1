import sys
import socket
import threading
from random import random
from time import sleep
import Queue

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def main():
    print bcolors.HEADER + " --- Creating Network Layer --- " + bcolors.ENDC
    if len(sys.argv) < 7:
        print bcolors.FAIL + "Not enough arguments provided. Need 7, got " + str(len(sys.argv)) + bcolors.ENDC
        exit(1)

    global TCP_IP
    global servers
    global delay
    global queues
    TCP_IP = socket.gethostbyname(socket.gethostname())
    TCP_PORT = int(sys.argv[1])
    delay = float(sys.argv[6])
    BUFFER_SIZE = 1024
    servers = {
        "A": int(sys.argv[2]),
        "B": int(sys.argv[3]),
        "C": int(sys.argv[4]),
        "D": int(sys.argv[5])
    }
    queues = {
        "A": (Queue.Queue(), threading.Condition()),
        "B": (Queue.Queue(), threading.Condition()),
        "C": (Queue.Queue(), threading.Condition()),
        "D": (Queue.Queue(), threading.Condition())
    }

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    print bcolors.HEADER + " --- Network Layer Created --- " + bcolors.ENDC
    s.listen(1)
    while 1:
        conn, addr = s.accept()
        data = conn.recv(BUFFER_SIZE)
        t = threading.Thread(target=handle_message, args=(data, conn))
        t.daemon = True
        t.start()
    conn.close()


def handle_message(msg, conn):
    msg = msg.split()
    if len(msg) < 3 or msg[-1] not in servers.keys():
        conn.send("Malformed message")
        conn.close()
        print bcolors.WARNING + "Malformed Message" + bcolors.ENDC
        return

    queues[msg[-1]][0].put(threading.current_thread())
    if msg[0] == "Send":
        sleep(random()*delay)
        queues[msg[-1]][1].acquire()
        while threading.current_thread() != queues[msg[-1]][0].queue[0]:
            queues[msg[-1]][1].wait()
        SEND_PORT = servers[msg[-1]]
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_socket.connect((TCP_IP, SEND_PORT))
        send_socket.send(" ".join(msg[1:-1]))
        send_socket.close()
        print bcolors.OKBLUE + "Sent Message" + bcolors.ENDC
        queues[msg[-1]][0].get()
        queues[msg[-1]][1].notifyAll()
        queues[msg[-1]][1].release()

    else:
        conn.send("Malformed message")
        print bcolors.WARNING + "Malformed Message" + bcolors.ENDC

    conn.close()


if __name__ == '__main__':
    main()

