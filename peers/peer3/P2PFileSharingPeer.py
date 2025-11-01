import os
import socket as s
import sys
import threading
import common as c
import time

if len(sys.argv) < 5:
    print("Usage: python P2PFileSharingPeer <Server IP>:<Server Port Number> <File Repository> <Schedule File>")
    sys.exit(1)

s_info = sys.argv[1].split(':')
server_ip = s_info[0]
server_port = int(s_info[1])
file_repository = sys.argv[2]
schedule_file = sys.argv[3]
#serving_port = int(sys.argv[4])

port = 10000
#servingPort = serving_port
maxConnections = 250

schedule_file_contents = {}
schedule_files = {}
files = []
active_search_request = None
active_file_lock = threading.Lock()
active_file_done_counter = 0
active_search_request_remaining_parts = {}
search_done_event = threading.Event()
search_lock = threading.Lock()
wait_amount = 0

server_socket = s.socket(s.AF_INET, s.SOCK_STREAM)
serving_socket = s.socket(s.AF_INET, s.SOCK_STREAM)

peer_sockets = {}
try:
    serving_socket.bind(('0.0.0.0', 0))
    serving_socket.listen(maxConnections)
    serving_socket.settimeout(1)
    server_socket.connect((server_ip, server_port))
except s.error as e:
    print(f"Error binding: {e} or port {port}")
    #sys.exit(1)

try:
    open("download.log", "w").close()
    with open(schedule_file, "r") as f:
        schedule_file_temp = f.readlines()
        for line in schedule_file_temp:
            if line.startswith("wait"):
                wait_amount = line.split(' ')[1][:-1]
                continue
            args = line.split(':')
            schedule_file_contents[args[0]] = int(args[1][:-1])
            schedule_files[args[0]] = False
            #print(args[0])
    files = os.listdir(file_repository)
# for file in files:
#print(file)
except FileNotFoundError:
    print(f"Error: The file '{schedule_file}' was not found.")
except Exception as e:
    print(f"An error occurred while reading the file: {e}")


def startServing():
    ip = serving_socket.getsockname()[0]
    s_port = serving_socket.getsockname()[1]
    command = f"START SERVING {s_port} END"
    server_socket.send(command.encode())


def sendFileRepoToServer(_files):
    command = f"START PROVIDING {str(serving_socket.getsockname()[1])} {len(_files)} "
    for file_rep in _files:
        command += file_rep + " "
    command += "END"
    print(command)
    server_socket.send(command.encode())


def sendPartialDownloadRequest(conn, file_partial, start_byte, end_byte):
    command = f"START DOWNLOAD {file_partial} {start_byte} {end_byte} END"
    try:
        active_search_request_remaining_parts[conn] = {"start": start_byte, "end": end_byte}
        print(f"I am {file_repository} and Sending download request to peer : " + command)
        conn.send(command.encode('utf-8'))
    except s.error as e:
        print(f"Error sending download request: {e}")


def sendSearchRequest(file_search, size):
    global active_search_request
    global active_search_request_remaining_parts
    if active_search_request is not None:
        search_done_event.wait()
        search_done_event.clear()
    active_search_request_remaining_parts = {}
    active_search_request = [file_search, size]
    search_done_event.clear()
    try:
        command = f"START SEARCH {file_search} END"
        server_socket.send(command.encode('utf-8'))
        print(f"Sent search request for '{file_search}' to server from {file_repository}.")
        print(f"Sent search request for '{file_search}' to server.")
    except s.error as e:
        print(f"Error sending search request: {e}")
        resetActiveFileRequest()
        sendSearchRequest(file_search, size)
    except Exception as e:
        print("An error occurred while sending search request")


def connectWithPeer(ip, port_peer):
    global peer_sockets
    try:
        sock = s.socket(s.AF_INET, s.SOCK_STREAM)
        sock.connect((ip, int(port_peer)))
        return sock
    except s.error as e:
       # print(f"Error connecting from connectiwthpeer to server: {e}")
        print(f"Error connecting to server: {e}")
        return None


def calculateStartEndBytes(currIdx, providerCount):
    global active_search_request
    fileSize = schedule_file_contents[active_search_request[0]]
    base_size = fileSize // providerCount
    remainder = fileSize % providerCount

    bytes_from_base = currIdx * base_size
    bytes_from_remainder = min(currIdx, remainder)

    start_byte = bytes_from_base + bytes_from_remainder
    curr_chunk_length = base_size
    if currIdx < remainder:
        curr_chunk_length += 1
    end_byte = start_byte + curr_chunk_length - 1
    if end_byte >= fileSize:
        end_byte = fileSize - 1
    print(f"Current start byte: {start_byte} and current end byte: {end_byte}")
    return start_byte, end_byte


def processProviderInfo(providers):
    global active_search_request
    providerCount = len(providers)
    print(f"Processing {providerCount} providers.")
    currIdx = 0
    for provider in providers:
        print(f"Trying to connect with {str(providers)}")
        provider_info = provider.split(':')
        sock = connectWithPeer(provider_info[0], provider_info[1])
        print(f"Connected with {str(providers)}")
        if sock is not None:
            start_byte, end_byte = calculateStartEndBytes(currIdx, providerCount)
            sendPartialDownloadRequest(sock, active_search_request[0], start_byte, end_byte)
            currIdx += 1
            thread = threading.Thread(target=downloadListener, args=(sock, 0), daemon=True)
            thread.start()
        else:
            print(f"Sock is none cant send download request")


def processDataFromServer(data):
    dataStr = data.decode("utf-8")
    arguments = dataStr.split(' ')
    if len(arguments) > 2 and arguments[1] == "PROVIDERS":
        # print(str(arguments))
        print("Providers from server: " + str(arguments))
        providers = arguments[2:-1]
        processProviderInfo(providers)


def sendRequestedFile(conn, file_requested, start_byte, end_byte):
    try:
        print(f"sendRequestedFile called for {file_requested}, {start_byte}-{end_byte}")
        print(f"Connection info: {conn.getpeername()}")
        with open(f"{file_repository}\\{file_requested}", "rb") as f:
            f.seek(start_byte, 0)
            chunk_size = end_byte - start_byte + 1
            data_to_send = f.read(chunk_size)
            conn.sendall(data_to_send)
            print(
                f"Sent file '{file_requested}' to peer with size of {len(data_to_send)} and peer {conn.getpeername()}")
            return True
    except s.error as e:
        print(f"Error sending download request: {e}")
    except Exception as e:
        print(f"An error occurred while sending requested file {e}")


def resetActiveFileRequest():
    global active_search_request
    global active_search_request_remaining_parts
    global active_file_done_counter
    active_search_request = None
    active_file_done_counter = 0
    search_done_event.set()


def writeReceivedData(conn, data):
    global active_file_done_counter
    isDownloadFinished = False

    try:
        full_path = os.path.join(file_repository, active_search_request[0])
        if not os.path.exists(full_path):
            file_path = f"{file_repository}\\{active_search_request[0]}"
            with open(file_path, "wb") as f:
                a = 5

        with open(f"{file_repository}\\{active_search_request[0]}", "rb+") as f:
            with active_file_lock:
                active_file_info = active_search_request_remaining_parts[conn]
                start_byte = active_file_info["start"]
                end_byte = active_file_info["end"]
            f.seek(start_byte, 0)
            f.write(data)
            with active_file_lock:
                active_file_info["start"] = (start_byte + len(data))
                if start_byte + len(data) >= end_byte:
                    print(f"Received data from {conn.getpeername()} and i am peer {file_repository} and got {f.name}")
                    #print(f"{str(active_file_info)} for {f.name}")
                    active_file_done_counter += 1
                    isDownloadFinished = True
                    with open("download.log", "a") as f_download:
                        f_download.write(f"{active_search_request[0]} {conn.getpeername()[0]}:{conn.getpeername()[1]}\n")
                    #print(f"done cnt {active_file_done_counter} and total cnt {len(active_search_request_remaining_parts)}")
                    if active_file_done_counter >= len(active_search_request_remaining_parts):
                        print(f"File {active_search_request[0]} downloaded successfully to {file_repository}")
                        schedule_files[active_search_request[0]] = True
                        sendFileRepoToServer([active_search_request[0]])
                        countD = 0
                        for key in schedule_files.keys():
                            if schedule_files[key]:
                                countD += 1
                        if countD == len(schedule_files):
                            with open("done", "wb") as f:
                                print(f"Created done for {file_repository}")
                        resetActiveFileRequest()
    except s.error as e:
        print(f"Error writing file: {e}")
    return isDownloadFinished


def processDataFromPeer(conn, data):
    if data.startswith(b"START"):
        try:
            dataStr = data.decode("utf-8")
            arguments = dataStr.split(' ')
            if len(arguments) > 2 and arguments[1] == "DOWNLOAD":
                sendRequestedFile(conn, arguments[2], int(arguments[3]), int(arguments[4]))
        except Exception as e:
            print(f"Error processing command: {e}")


def peerListener(conn, a):
    while True:
        try:
            data = conn.recv(1024)
            processDataFromPeer(conn, data)
        except s.error as e:
            print(f"Error peer listener: {e}")
            break


def downloadListener(conn, a):
    chunk_size = 1024 * 1024
    while True:
        try:
            data = conn.recv(chunk_size)
            if not data:
                with active_file_lock:
                    active_search_request_remaining_parts.pop(conn, None)
                conn.close()
                break
            if writeReceivedData(conn, data):
                conn.close()
                break
        except s.error as e:
            print(f"Error downloading file: {e}")
            with active_file_lock:
                active_search_request_remaining_parts.pop(conn, None)
                conn.close()
                break


def serverListener():
    while True:
        try:
            data = server_socket.recv(4096)
            processDataFromServer(data)
        except s.error as e:
            print(f"Error on server listener: {e}")
            break


def peerAcceptor():
    while True:
        try:
            conn, addr = serving_socket.accept()
            thread = threading.Thread(target=peerListener, args=(conn, 0), daemon=True)
            thread.start()
        except s.timeout:
            #  print("Timed out")
            continue
        except s.error as e:
            print(f"error acceptor {e}")
            continue
        except Exception as e:
            print(f"error acceptor {e}")
            continue


def startSchedule():
    for file_name in schedule_file_contents.keys():
        file_size = schedule_file_contents[file_name]
        #print(f"{file_name}: {file_size} bytes schedule")
        sendSearchRequest(file_name, file_size)


def startPeer():
    print("Starting Peer")
    server_listener_thread = threading.Thread(target=serverListener, daemon=True)
    server_listener_thread.start()
    print("Server Listener started")

    peer_acceptor_thread = threading.Thread(target=peerAcceptor, daemon=True)
    peer_acceptor_thread.start()
    print("Peer Acceptor started")

    startServing()
    print("START SERVING command has been sent")

    sendFileRepoToServer(files)
    print("START PROVIDING command has been sent")
    time.sleep(5)
    now = time.time()
    startSchedule()
    diff = time.time() - now
    print(str(diff) + " has been passed for : " + file_repository)
    while True:
        time.sleep(100)


if __name__ == "__main__":
    try:
        startPeer()
    except Exception as e:
        print(f"Fatal Error starting peer {e}")
