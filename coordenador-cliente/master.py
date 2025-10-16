import socket
from _thread import start_new_thread
import threading
import json
import time

#CONSTANTS

HOST = "10.62.217.212"
PORT = 5000

MASTERS = {
  "servers": [
    {
      "ip": "10.62.217.199",
      "name": "joao"
    },
    {
      "ip": "10.62.217.16",
      "name": "thales.martins"
    },
    {
      "ip": "10.62.217.209",
      "name": "thiago.machado"
    },
    {
      "ip": "10.62.217.203",
      "name": "thiago.filho"
    }
  ]
}

#WORKER

QUERY_WORKER = {
  "TASK": "QUERY",
  "USER": "11111111111"
}

#MASTER

SEND_ALIVE_MASTER = {
  "MASTER": "rodrigo.everton",
  "TASK": "HEARTBEAT"
}

RESPOND_ALIVE_MASTER = {
  "MASTER": "rodrigo.everton",
  "TASK": "HEARTBEAT",
  "RESPONSE":"ALIVE"
}

ASK_FOR_WORKERS = {
  "MASTER": "rodrigo.everton",
  "TASK": "WORKER_REQUEST"
}

ASK_FOR_WORKERS_RESPONSE_NEGATIVE = {
  "MASTER": "rodrigo.everton",
  "RESPONSE": "UNAVAILABLE"
}

ASK_FOR_WORKERS_RESPONSE_POSITIVE = {
  "MASTER": "[ID_i]",
  "RESPONSE": "AVAILABLE",
  "WORKERS": {"WORKER_UUID":"uuid"}
}

#VARIABLES

masters_alive = {0}
workers_received = {0}
workers_lent = {0}
workers_controlled = {0}
errorCounter = 0

#FUNCTIONS

def send_json(conn, obj):
    data = json.dumps(obj) + "\n"
    conn.sendall(data.encode("utf-8"))

def check_counter():
  while True:
    time.sleep(10)
    if errorCounter >= 10:
      ask_for_workers()

def send_alive_master():
    while True:
        servers = MASTERS["servers"]
        for server in servers:
            ip = server["ip"]
            name = server["name"]

            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, PORT))
                    print(f"sending ALIVE to '{name}' at '{ip}:{PORT}'")
                    send_json(s, SEND_ALIVE_MASTER)
            except Exception as e:
                print(f"failed to connect to '{name}' at '{ip}:{PORT}'")

def receive_alive_master(c, addr):
    data = c.recv(1024)
    
    if not data:
      print('data not found')
      return
    
    send_json(c, RESPOND_ALIVE_MASTER)
    masters_alive.add(addr[0])
    c.close()
    
def listen_masters():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    print(f"server running on '{HOST}:{PORT}'")
    
    while True:
        c, addr = s.accept()
        servers = MASTERS["servers"]
        for server in servers:
            ip = server["ip"]
            name = server["name"]
            if ip == addr[0]:
                print(f"receiving connection from SERVER '{name}' at '{ip}:{addr[1]}'")
                threading.Thread(target=receive_alive_master, daemon=True, args=(c, addr,)).start()
                time.sleep(10)

def ask_for_workers():
    global errorCounter
    for host in masters_alive:
      try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
          s.connect((host, PORT))
          
          name = "not identified"
          for server in MASTERS["servers"]:
            if server["ip"] == host:
              name = server["name"]
              break
            
          print(f"ASK_FOR_WORKERS to '{name}' at '{host}:{PORT}'")
          send_json(s, ASK_FOR_WORKERS)
      except Exception as e:
        print(f"failed to connect to '{name}' at '{host}:{PORT}'")

      data = s.recv(1024)
      response = json.loads(data)
      status = response.get("RESPONSE")

      if status == "AVAILABLE":
        workers = response["WORKERS"]
        for worker in workers:
          #TODO: dicionario?
          workers_received.add(worker)
          workers_controlled.add(worker)
      s.close()
      errorCounter = 0     
      return

def send_workers():
  

def receive_balance(c, addr):
  global errorCounter
  data = c.recv(1024)

  if not data:
    print('data not found')
    errorCounter += 1
    return

  if data.get("STATUS") == "OK":
    saldo = data.get("SALDO")
    print(f"saldo: R${saldo}")
    workers_controlled.add(addr[0])
  elif data.get("STATUS") == "NOK":
    erro = data.get("ERROR")
    print(f"error from worker: {erro}")
    errorCounter += 1
  else:
    errorCounter += 1
        
def receive_alive_worker(c, addr):
    data = c.recv(1024)
    
    if not data:
      print('data not found')
      return
    
    send_json(c, QUERY_WORKER)
    receive_balance(c, addr)
    c.close()

def listen_workers():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT + 1))
    s.listen()
    while True:
        c, addr = s.accept()
        print(f"receiving connection from WORKER '{addr[0]}:{addr[1]}'")
        threading.Thread(target=receive_alive_worker, daemon=True, args=(c, addr,)).start()
        time.sleep(1)

def main():
    send_alive_thread = threading.Thread(target=send_alive_master)
    receive_alive_thread = threading.Thread(target=listen_masters)
    listen_workers_thread = threading.Thread(target=listen_workers)

    send_alive_thread.start()
    receive_alive_thread.start()
    listen_workers_thread.start()

if __name__ == '__main__':
  main()