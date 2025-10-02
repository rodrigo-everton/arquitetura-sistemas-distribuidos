import socket
from _thread import start_new_thread
import threading
import json
import time

HOST = "10.62.217.212"
PORT = 5000

WORKERS = ["10.62.217.21"]

MASTERS = {
  "servers": [
    {
      "ip": "10.62.217.199",
      "port": 5000,
      "name": "joao"
    },
    {
      "ip": "10.62.217.16",
      "port": 5000,
      "name": "thales.martins"
    },
    {
      "ip": "10.62.217.209",
      "port": 5000,
      "name": "thiago.machado"
    },
    {
      "ip": "10.62.217.203",
      "port": 5000,
      "name": "thiago.filho"
    }
  ]
}


SEND_ALIVE = {
  "SERVER_ID": "rodrigo.everton",
  "TASK": "HEARTBEAT"
}

RESPOND_ALIVE = {
  "SERVER_ID": "rodrigo.everton",
  "TASK": "HEARTBEAT"
}

ASK_FOR_WORKERS = {
  "TASK": "WORKER_REQUEST",
  "WORKERS_NEEDED": 5
}

ASK_FOR_WORKERS_RESPONSE_NEGATIVE = {
  "TASK": "WORKER_RESPONSE",
  "STATUS": "NACK",
  "WORKERS": []
}

ASK_FOR_WORKERS_RESPONSE_POSITIVE = {
  "TASK": "WORKER_RESPONSE",
  "STATUS": "ACK",
  "MASTER_UUID": "UUID",
  "WORKERS": [
    {"WORKER_UUID": "..."},
    {"WORKER_UUID": "..."}
  ]
}

QUERY_WORKER = {
  "TASK": "QUERY",
  "USER": "11111111111"
}

masters_alive = []
workers_received = {}
workers_lent = {}
workers_controlled = {}

def send_json(conn, obj):
    data = json.dumps(obj) + "\n"
    conn.sendall(data.encode("utf-8"))

def send_alive():
    while True:
        servers = MASTERS["servers"]
        for server in servers:
            ip = server["ip"]
            port = server["port"]
            name = server["name"]

            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, port))
                    print(f"sending ALIVE to '{name}' at {ip}:{port}")
                    send_json(s, SEND_ALIVE)
            except Exception as e:
                print(f"failed to connect to {ip}:{port} ({name})")

def receive_alive_master(c):
    data = c.recv(1024)
    if not data:
        print('data not found')
    send_json(c, RESPOND_ALIVE)
    response = json.loads(data)
    masters_alive.append(response.get("SERVER_ID"))
    c.close()
    
def listen_masters():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))  # DIFFERENT PORT to avoid conflict
    s.listen()
    print(f"server running on '{HOST}:{PORT}'")
    
    while True:
        c, addr = s.accept()
        print('receiving connection from:', addr[0], ':', addr[1])
        threading.Thread(target=receive_alive_master, daemon=True, args=(c,)).start()
        time.sleep(10)

#TODO: todo quebrado
def ask_for_workers(c):
    for host in masters_alive:
        c.connect((host, PORT))

    name = "not identified"
    for server in MASTERS["servers"]:
        if server["ip"] == host:
            name = server["name"]
            break
            
        print(f"requesting workers to '{name}' at '{host}:{PORT}'")
        send_json(c, ASK_FOR_WORKERS)

        data = c.recv(1024)
        response = json.loads(data)
        status = response.get("STATUS")

        if status == "ACK":
            workers = response["WORKERS"]
            for worker in workers:
                host = WORKERS.get("WORKER_UUID") #adiciona de workers-conhecidos para workers-controlados
                workers_controlled[id] = host
            return
        else:
            return
        
def receive_alive_worker(c):
    while True:
        data = c.recv(1024)
        time.sleep(1)
        if not data:
            print('data not found')
            break
        send_json(c, QUERY_WORKER)
    c.close()

def listen_workers():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT + 2))  # DIFFERENT PORT to avoid conflict
    s.listen()
    while True:
        c, addr = s.accept()
        print('receiving connection from:', addr[0], ':', addr[1])
        threading.Thread(target=receive_alive_worker, daemon=True, args=(c,)).start()

def main():
    send_alive_thread = threading.Thread(target=send_alive)
    receive_alive_thread = threading.Thread(target=listen_masters)
    listen_workers_thread = threading.Thread(target=listen_workers)

    send_alive_thread.start()
    receive_alive_thread.start()
    listen_workers_thread.start()

if __name__ == '__main__':
    main()