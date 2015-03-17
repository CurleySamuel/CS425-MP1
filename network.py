import sys
import socket
import threading
import thread
from random import random
from time import sleep
import Queue
import pickle

# Helper class used to colorize our print messages.
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
    # We pass the delay dictionary pickled as a command line argument.
    delay = pickle.loads(sys.argv[6])
    BUFFER_SIZE = 1024
    # These command line arguments are the ports the servers will be listening on.
    servers = {
        "A": int(sys.argv[2]),
        "B": int(sys.argv[3]),
        "C": int(sys.argv[4]),
        "D": int(sys.argv[5])
    }
    # Each server gets a,
    # 1. Queue for messages. This is used to launch threads for each message.
    # 2. Queue containing the thread_ids for each message. Use this to determine send order.
    # 3. Condition variable. This is used to prevent threads from busy waiting to check if they're next.
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
    # We launch a handling thread for every server. These threads will watch the first queue for new msgs.
    for x in ["A", "B", "C", "D"]:
        t = threading.Thread(target=thread_function, args=(queues[x])+(x,))
        t.daemon = True
        t.start()
    print bcolors.HEADER + " --- All Sending Threads Created --- " + bcolors.ENDC
    s.listen(1)
    print bcolors.HEADER + " --- Network Layer Created --- " + bcolors.ENDC

    # Listen baby listen. Everything that comes in will be either a,
    # 1. Send. Then do a little validation then throw it onto the queue.
    # 2. BCAST. If it's a BCAST then we convert it into 4 send messages, one for each server.
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
            # Put it onto the appropriate queue
            queues[msg[-1].upper()][0].put(msg)
            conn.close()
        elif msg[0].lower() == "bcast":
            msg[0] = "send"
            msg.append("A")
            for x in ["A", "B", "C", "D"]:
                msg[-1] = x
                # Need the list() here to create a new object. Otherwise every thread will have a
                # reference to the same object and everything screws up. Was a fun bug.
                queues[x][0].put(list(msg))
                conn.close()
        else:
            bad_message(conn)


# Quick helper function that'll handle the case of a malformed message.
def bad_message(conn):
    conn.send("Malformed message")
    conn.close()
    print bcolors.WARNING + "Malformed Message" + bcolors.ENDC


# A thread for every server is launched into this function. These four threads will listen to their
# respective queue and launch a thread for every incoming message.
def thread_function(q1, q2, q3, me):
    q = (q1, q2, q3)
    while 1:
        msg = q[0].get()
        # We need to acquire the lock before modifying the queue.
        q[2].acquire()
        t = threading.Thread(target=send_message, args=(msg, (q[1], q[2]), me))
        t.daemon = True
        t.start()
        q[1].put(t.ident)
        # After putting an element on the queue we need to notify and release the lock.
        q[2].notifyAll()
        q[2].release()


# A thread for every message will launch into this function.
# msg: A list created by splitting the message by spaces
# q: The queue object that the thread_ids were put onto
# me: The origin server. Used to look up channel delays.
def send_message(msg, q, me):
    # Sleep that random amount by looking up MAX from the delay dict.
    sleep(random()*delay[me][msg[-1]])
    q[1].acquire()
    # Use the conditional to wait for this message to be next on the queue.
    while thread.get_ident() != q[0].queue[0]:
        q[1].wait()
    # It's my turn! Send the message and notify all other waiting threads.
    SEND_PORT = servers[msg[-1].upper()]
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_socket.connect((TCP_IP, SEND_PORT))
    send_socket.send(" ".join(msg[1:-1]))
    send_socket.close()
    print bcolors.OKBLUE + "Sent " + bcolors.OKGREEN + msg[2] + bcolors.OKBLUE + " (" + me + " -> " + msg[-1]+ ")" + bcolors.ENDC
    q[0].get()
    q[1].notifyAll()
    q[1].release()


# Don't have to worry about functions not being defined yet if you create a main and just jump to it.
if __name__ == '__main__':
    main()

