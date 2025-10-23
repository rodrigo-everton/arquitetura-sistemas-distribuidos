import socket
from _thread import start_new_thread
import threading
import json
import time
import random

#CONSTANTS

HOST = "127.0.0.2"
PORT = 5000

MASTERS = {
  "servers": [
    {
      "ip": "127.0.0.1",
      "name": "primeiro"
    },
  ]
}

#WORKER

QUERY_WORKER = {
  "TASK": "QUERY",
  "USER": "11111111111"
}

SEND_WORKER = {
  "MASTER": "[2]",
  "TASK": "REDIRECT",
  "MASTER_REDIRECT": [0]
}

#MASTER

SEND_ALIVE_MASTER = {
  "MASTER": "2",
  "TASK": "HEARTBEAT"
}

RESPOND_ALIVE_MASTER = {
  "MASTER": "2",
  "TASK": "HEARTBEAT",
  "RESPONSE":"ALIVE"
}

ASK_FOR_WORKERS = {
  "MASTER": "[2]",
  "TASK": "WORKER_REQUEST"
}

ASK_FOR_WORKERS_RESPONSE_NEGATIVE = {
  "MASTER": "[2]",
  "RESPONSE": "UNAVAILABLE"
}

ASK_FOR_WORKERS_RESPONSE_POSITIVE = {
  "MASTER": [2],
  "RESPONSE": "AVAILABLE",
  "WORKERS": [{"WORKER_UUID":"uuid"}]
}

#VARIABLES

masters_alive = {0}
masters_alive_dict = dict()
workers_received = dict()
workers_lent = {0}
workers_controlled = dict()
errorCounter = 10

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
            time.sleep(1)

            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, PORT))
                    print(f"sending ALIVE to '{name}' at '{ip}:{PORT}'")
                    send_json(s, SEND_ALIVE_MASTER)
            except Exception as e:
                print(f"failed to connect to SERVER '{name}' at '{ip}:{PORT}'")

def receive_alive_master(c, addr):
    raw_data = c.recv(1024)
    
    if not raw_data:
      print('data not found')
      return

    try:
      data = json.loads(raw_data.decode())
    except Exception as e:
      print(f"Failed to parse JSON: {e}")
      return

    if data["TASK"] == "WORKER_REQUEST":
        send_workers(addr[0])
        return
    
    send_json(c, RESPOND_ALIVE_MASTER)
    masters_alive.add(addr[0])
    masters_alive_dict[data.get("MASTER")[0]] = addr[0]
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
    name = "not identified"
    for host in masters_alive:
      try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
          s.connect((host, PORT))
          
          for server in MASTERS["servers"]:
            if server["ip"] == host:
              name = server["name"]
              break
            
          print(f"ASK_FOR_WORKERS to '{name}' at '{host}:{PORT}'")
          send_json(s, ASK_FOR_WORKERS)

          data = s.recv(1024)
          response = json.loads(data)
          status = response.get("RESPONSE")

          if status == "AVAILABLE":
            workers = response["WORKERS"]
            for worker in workers:
                key = list(worker.keys())[0]
                value = worker[key]
                workers_controlled[key] = value
                workers_controlled[key] = value
          errorCounter = 0     
          return
      except Exception as e:
        print(f"failed to connect to SERVER '{name}' at '{host}:{PORT}' with error: {e}")

def send_workers(addr):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

  random_key = random.choice(list(workers_controlled.keys()))
  worker_host = workers_controlled[random_key]

  s.bind((worker_host, PORT + 1))
  s.listen()

  send_worker = SEND_WORKER
  for key, val in masters_alive_dict.items():
    if val == addr:
      master_id = key
  send_worker["MASTER_REDIRECT"] = [masters_alive_dict[master_id]]
  send_json(s, send_worker)

def receive_balance(c, addr):
  global errorCounter
  raw_data = c.recv(1024)

  if not raw_data:
    print('data not found')
    errorCounter += 1
    return

  try:
    data = json.loads(raw_data.decode())
    print(data)
  except Exception as e:
    print(f"Failed to parse JSON: {e}")
    errorCounter += 1
    return

  if data.get("STATUS") == "OK":
    saldo = data.get("SALDO")
    print(f"saldo: R${saldo}")
  elif data.get("STATUS") == "NOK":
    erro = data.get("ERROR")
    print(f"error from WORKER: {erro}")
    errorCounter += 1
  else:
    errorCounter += 1
        
def receive_alive_worker(c, addr):
  raw_data = c.recv(1024)

  if not raw_data:
    print('data not found')
    return

  try:
    data = json.loads(raw_data.decode())
    workers_controlled[data.get("WORKER_UUID")] = addr[0]
  except Exception as e:
    print(f"Failed to parse JSON: {e}")
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
    check_counter_thread = threading.Thread(target=check_counter)
    send_alive_thread = threading.Thread(target=send_alive_master)
    receive_alive_thread = threading.Thread(target=listen_masters)
    listen_workers_thread = threading.Thread(target=listen_workers)

    check_counter_thread.start()
    send_alive_thread.start()
    receive_alive_thread.start()
    listen_workers_thread.start()

if __name__ == '__main__':
  main()