import sys
import socket
import threading
import thread
from random import random
from time import sleep
import Queue
import multiprocessing

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
    print bcolors.HEADER + " --- Initializing Network Layer --- " + bcolors.ENDC
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
        "A": (Queue.Queue(), Queue.Queue(), threading.Condition()),
        "B": (Queue.Queue(), Queue.Queue(), threading.Condition()),
        "C": (Queue.Queue(), Queue.Queue(), threading.Condition()),
        "D": (Queue.Queue(), Queue.Queue(), threading.Condition())
    }

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    print bcolors.HEADER + " --- Binding to Socket --- " + bcolors.ENDC
    for x in ["A", "B", "C", "D"]:
        t = threading.Thread(target=thread_function, args=(queues[x]))
        t.daemon = True
        t.start()
    print bcolors.HEADER + " --- All Sending Threads Created --- " + bcolors.ENDC
    s.listen(1)
    print bcolors.HEADER + " --- Network Layer Created --- " + bcolors.ENDC

    while 1:
        conn, addr = s.accept()
        data = conn.recv(BUFFER_SIZE)
        msg = data.split()
        if len(msg) < 2:
            bad_message(conn)
        elif msg[0].lower() == "send":
            if msg[-1].upper() not in servers.keys() or len(msg) < 3:
                bad_message(conn)
                continue
            queues[msg[-1].upper()][0].put(msg)
            conn.close()
        elif msg[0].lower() == "bcast":
            msg[0] = "send"
            msg.append("A")
            for x in ["A", "B", "C", "D"]:
                msg[-1] = x
                queues[x][0].put(list(msg))
                conn.close()
        else:
            bad_message(conn)


def thread_function(q1, q2, q3):
    q = (q1, q2, q3)
    while 1:
        msg = q[0].get()
        q[2].acquire()
        t = threading.Thread(target=send_message, args=(msg, (q[1], q[2])))
        t.daemon = True
        t.start()
        q[1].put(t.ident)
        q[2].notifyAll()
        q[2].release()


def bad_message(conn):
    conn.send("Malformed message")
    conn.close()
    print bcolors.WARNING + "Malformed Message" + bcolors.ENDC


def send_message(msg, q):
    sleep(random()*delay)
    q[1].acquire()
    while thread.get_ident() != q[0].queue[0]:
        q[1].wait()
    SEND_PORT = servers[msg[-1].upper()]
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_socket.connect((TCP_IP, SEND_PORT))
    send_socket.send(" ".join(msg[1:-1]))
    send_socket.close()
    print bcolors.OKBLUE + "Sent Message" + bcolors.ENDC
    q[0].get()
    q[1].notifyAll()
    q[1].release()


if __name__ == '__main__':
    main()

