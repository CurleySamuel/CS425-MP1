import sys
import socket
import threading

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
    TCP_IP = socket.gethostbyname(socket.gethostname())
    TCP_PORT = int(sys.argv[1])
    BUFFER_SIZE = 1024
    servers = {
        "A": int(sys.argv[2]),
        "B": int(sys.argv[3]),
        "C": int(sys.argv[4]),
        "D": int(sys.argv[5])
    }

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    print bcolors.OKGREEN + "Network Layer Created" + bcolors.ENDC
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

    if msg[0] == "Send":
        SEND_PORT = servers[msg[-1]]
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_socket.connect((TCP_IP, SEND_PORT))
        send_socket.send(" ".join(msg[1:-1]))
        send_socket.close()
        conn.send("ACK")
        print bcolors.OKBLUE + "Sent Message" + bcolors.ENDC

    else:
        conn.send("Malformed message")
        print bcolors.WARNING + "Malformed Message" + bcolors.ENDC

    conn.close()


if __name__ == '__main__':
    main()

