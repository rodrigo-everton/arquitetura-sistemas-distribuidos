import socket
from _thread import start_new_thread
import threading
import json
import time

HOST = "127.0.0.1"
PORT = 8000

WORKERS = []

MASTERS = {"joao" : "10.62.217.99", "thales.martins" : "10.62.217.217",
"thiago.filho" : "10.62.217.204"}

SEND_ALIVE = {
  "SERVER": "ALIVE",
  "TASK": "REQUEST"
}

RESPOND_ALIVE = {
  "SERVER": "ALIVE",
  "TASK": "RECIEVE"
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

masters_alive = []
workers_received = {}
workers_lent = {}
workers_controlled = {}

def send_json(conn, obj):
    data = json.dumps(obj) + "\n"
    conn.sendall(data.encode("utf-8"))

def send_alive(c):
    for key, value in MASTERS():
        c.connect((value, PORT))
        print(f"sending alive to '{key}' at '{value}:{PORT}'")

    send_json(c, SEND_ALIVE)
    
def receive_alive(c):
    while True:
        data = c.recv(1024)
        time.sleep(30)
        if not data:
            print('data not found')
            break
        send_json(c, RESPOND_ALIVE)
    c.close()
    
def listen_masters(s):
    s.listen(5)
    print(f"server running on '{HOST}:{PORT}'")
    
    while True:
        c, addr = s.accept()
        print('receiving connection from:', addr[0], ':', addr[1])
        threading.Thread(target=receive_alive, daemon=True, args=(c,)).start()

def ask_for_workers(c):
    for host in masters_alive:
        c.connect((host, PORT))

        name = "not identified"
        for key in MASTERS():
            if MASTERS.get(key) == host:
                name = key
                break
            
        print(f"requesting workers to '{name}' at '{host}:{PORT}'")
        send_json(c, ASK_FOR_WORKERS)

        data = c.recv(1024)
        response = json.loads(data)
        status = response.get("STATUS")

        if status == "ACK":
            workers = response["WORKERS"]
            for id in workers:
                host = WORKERS.get(id) #adiciona de workers-conhecidos para workers-controlados
                workers_controlled[id] = host
            return
        else:
            return

def query_workers(c):
    for worker in workers_controlled:
        

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    
    send_alive_thread = threading.Thread(target=send_alive)
    receive_alive_thread = threading.Thread(target=listen_masters, args=(s,))
    query_workers_thread = threading.Thread(target=query_workers, args=(s,))

    send_alive_thread.start()
    receive_alive_thread.start()

if __name__ == '__main__':
    main()