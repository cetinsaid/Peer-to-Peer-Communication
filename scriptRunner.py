import common as c
import os
import subprocess
import sys
import time

SERVER_IP = "127.0.0.1"
SERVER_PORT = 10000
SERVER_SCRIPT = "P2pFileSharingServer.py"

PEER_LIST = [
    # (Peer ID, Serving Port, Repository Dir, Schedule File)
    ("peers\\peer1", 50001, "peer1-repo", "peer1-schedule.txt"),
    ("peers\\peer2", 50002, "peer2-repo", "peer2-schedule.txt"),
    ("peers\\peer3", 50003, "peer3-repo", "peer3-schedule.txt"),
]


def start_server():
    c.Log("Server is getting started")
    command = [sys.executable, SERVER_SCRIPT, str(SERVER_PORT)]
    process = subprocess.Popen(command, stdout=sys.stdout, stderr=sys.stderr)
    return process


def start_peer(peer_id, repo, schedule, port):
    c.Log(f"Starting Peer {peer_id} , {port}")
    peer_dir = peer_id
    command = [sys.executable, "P2PFileSharingPeer.py", f"{SERVER_IP}:{SERVER_PORT}", repo, schedule, str(port)]
    process = subprocess.Popen(command, cwd=peer_dir, stdout=sys.stdout, stderr=sys.stderr)
    return process


def main():
    server_process = None
    peer_processes = []

    try:
        server_process = start_server()
        time.sleep(2)
        for peer_id, serving_port, repo, schedule in PEER_LIST:
            p = start_peer(peer_id, repo, schedule, serving_port)
            peer_processes.append(p)
            time.sleep(1)

        print("\n--- ✅ Everything has begun ---")

        while True:
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n--- 🛑 Closing... ---")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        for p in peer_processes:
            if p.poll() is None:
                p.terminate()
                p.wait()

        if server_process and server_process.poll() is None:
            server_process.terminate()
            server_process.wait()

        print("--- Closed. ---")


if __name__ == "__main__":
    main()
