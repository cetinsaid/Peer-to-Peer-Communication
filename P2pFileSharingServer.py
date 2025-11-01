import socket as s
import sys
import threading
import common as c
import time

port = 8600
maxConnections = 100
if len(sys.argv) < 2:
    c.Log("Usage: python P2PFileSharingServer <Port Number>")
    sys.exit(1)
try:
    port = int(sys.argv[1])
except ValueError:
    c.Log("Error: Port Number must be an integer.")
    sys.exit(1)

connectionMap = {}
startRequest = {}
fileProviders = {}
startRequestSearcher = []
lock = threading.Lock()
waitAmountForSearch = 100
sock = s.socket(s.AF_INET, s.SOCK_STREAM)
try:
    sock.bind(('0.0.0.0', port))
    sock.listen(maxConnections)
    sock.settimeout(1.0)
except s.error as e:
    c.Log(f"Error binding or listening on port {port}: {e}")
    sys.exit(1)


def connectionMaker(socket, connLimit):
    socket.listen(connLimit)
    while True:
        try:
            conn, addr = socket.accept()
            if conn not in connectionMap:
                with lock:
                    connectionMap[conn] = {"addr": addr[0], "port": None}
                c.Log("Connection accepted for " + addr[0] + ":" + str(addr[1]))
                thread = threading.Thread(target=listenerThread, args=(conn,), daemon=True)
                thread.start()
        except s.timeout:
            #c.Log("Connection timed out")
            continue
        except s.error as e:
            c.Log(f"Error accepting a new connection: {e}")
            time.sleep(0.01)
            continue


def processRequest(request, conn):
    try:
        requestStr = request.decode('utf-8')
        arguments = requestStr.split(' ')
        c.Log("Received request: " + requestStr + " From " + conn.getpeername()[0])
        if len(arguments) == 4 and arguments[1] == "SERVING":
            with lock:
                connectionMap[conn]["port"] = int(arguments[2])
            c.Log("Port added " + str(connectionMap[conn]["port"]) + " For ip address " + conn.getpeername()[0])
        elif len(arguments) == 4 and arguments[1] == "SEARCH":
            c.Log("Search for file " + arguments[2] + " sent! Request is from " + conn.getpeername()[0])
            sendFileProviderInfoToPeer(conn, arguments[2])
        elif arguments[1] == "PROVIDING":
            with lock:
                saveFiles(conn, arguments)
        else:
            c.Log("No valid request received! : " + requestStr)
    except Exception as e:
        print(f"Error server processing request: {e}")


def saveFiles(conn, arguments):
    for i in range(4, len(arguments) - 1):
        if arguments[i] not in fileProviders:
            fileProviders[arguments[i]] = []
        fileProviders[arguments[i]].append([connectionMap[conn]["addr"], arguments[2]])
        c.Log("Saving file " + arguments[i])
        print("Saving file " + arguments[i] + " : " + arguments[2])


def sendFileProviderInfoToPeer(conn, file):
    sendInfo = "START PROVIDERS "
    for val in fileProviders[file]:
        sendInfo += str(val[0]) + ":" + str(val[1]) + " "
    sendInfo += "END"
    print("Sending file provider info to peer! : " + sendInfo)
    conn.send(sendInfo.encode('utf-8'))


def listenerThread(conn):
    while True:
        try:
            data = conn.recv(4096)
            processRequest(data, conn)
            time.sleep(0.001)
        except s.error as e:
           # print(f"Error listening on port {e}")
            continue

if __name__ == '__main__':
    try:
        connectionMaker(sock, maxConnections)
        c.Log("Server started!")
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        c.Log("Server is shutting down...")
        sock.close()
        sys.exit(0)
    except Exception as e:
        c.Log(f"Fatal error in main connection loop: {e}")
        sys.exit(1)
