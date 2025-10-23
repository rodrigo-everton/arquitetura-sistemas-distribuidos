import socket
from _thread import start_new_thread
import threading
from threading import Lock
import json
import time
import random

#CONSTANTS

HOST = "192.168.15.6"
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
  "MASTER": "[2]",
  "RESPONSE": "AVAILABLE",
  "WORKERS": {"WORKER_UUID":"uuid"}
}

THRESHOLD = 2

#VARIABLES

masters_alive = set()
masters_alive_dict = dict()
workers_received = dict()
workers_lent = set()
workers_controlled = dict()
workers_conns = dict()
workers_lock = Lock()

#FUNCTIONS

def send_json(conn, obj):
    data = json.dumps(obj) + "\n"
    conn.sendall(data.encode("utf-8"))

def check_threshold():
  while True:
    time.sleep(10)
    with workers_lock:
      if len(workers_controlled) >= THRESHOLD:
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
                print(f"failed to connect to SERVER '{name}' at '{ip}:{PORT}'")

def receive_alive_master(c, addr):
    raw_data = c.recv(1024)
    
    if not raw_data:
      print('data not found')
      c.close()
      return

    try:
      data = json.loads(raw_data.decode())
    except Exception as e:
      print(f"Failed to parse JSON: {e}")
      c.close()
      return 
    if data["TASK"] == "WORKER_REQUEST":
      success = send_workers(addr[0])
      if not success:
        print(f"failed to send workers to {addr[0]}")
        try:
          send_json(c, ASK_FOR_WORKERS_RESPONSE_NEGATIVE)
        except Exception as e:
          print(f"Failed to parse JSON: {e}")
          return
    
    send_json(c, RESPOND_ALIVE_MASTER)
    masters_alive.add(addr[0])
    masters_alive_dict[data.get("MASTER")] = addr[0]
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
                #time.sleep(10)     bloqueia novas conexões

def ask_for_workers():
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
            with workers_lock:
              for worker in workers:
                  key = list(worker.keys())[0]
                  value = worker[key]
                  workers_controlled[key] = value
                  workers_controlled[key] = value
          return
      except Exception as e:
        print(f"failed to connect to SERVER '{name}' at '{host}:{PORT}'")

def send_workers(addr):
  with workers_lock:
    if not workers_controlled:
       print("No workers available to send.")
       return
    random_key = random.choice(list(workers_controlled.keys()))
    worker_host = workers_controlled[random_key]
    worker_conn = workers_conns.get(random_key)

    if not worker_conn:
      print(f"No active connection found for worker {random_key}.")
      return

  send_worker = dict(SEND_WORKER)
  for key, val in masters_alive_dict.items():
    if val == addr:
      master_id = key
  send_worker["MASTER_REDIRECT"] = masters_alive_dict[master_id]
  try:
    send_json(worker_conn, send_worker)
    try:
      worker_conn.close()
    except Exception:
      pass
    with workers_lock:
      workers_controlled.pop(random_key, None)
      workers_conns.pop(random_key, None)
  except Exception as e:
    print(f"Failed to send workers to {addr}: {e}")

def receive_balance(c, addr):
  raw_data = c.recv(1024)

  if not raw_data:
    print('data not found')
    return

  try:
    data = json.loads(raw_data.decode())
    print(data)
  except Exception as e:
    print(f"Failed to parse JSON: {e}")
    return

  if data.get("STATUS") == "OK":
    saldo = data.get("SALDO")
    print(f"saldo: R${saldo}")
  elif data.get("STATUS") == "NOK":
    erro = data.get("ERROR")
    print(f"error from WORKER: {erro}")
  else:
    print("unknown STATUS from WORKER")
        
def receive_alive_worker(c, addr):
  raw_data = c.recv(1024)

  if not raw_data:
    print('data not found')
    c.close()
    return

  try:
    data = json.loads(raw_data.decode())
    worker_uuid = data.get("WORKER_UUID")
    if not worker_uuid:
      print("WORKER_UUID not found in data")
      c.close()
      return
    with workers_lock:
      workers_controlled[worker_uuid] = addr[0]
      workers_conns[worker_uuid] = c
  except Exception as e:
    print(f"Failed to parse JSON: {e}")
    c.close()
    return
  try:
    send_json(c, QUERY_WORKER)
    raw = c.recv(4096)
    if raw:
      try:
        resp = json.loads(raw.decode())
        print(resp)
      except Exception as e:
        print(f"Failed to parse worker response: {e}")
    receive_balance(c, addr)
  except Exception as e:
    print(f"Error communicating with worker: {e}")
    with workers_lock:
      workers_controlled.pop(worker_uuid, None)
      workers_conns.pop(worker_uuid, None)
    try:
      c.close()
    except Exception:
      pass
    return
  try:
    while True:
      more = c.recv(4096)
      if not more:
        break
      try:
        msg = json.loads(more.decode())
        print("worker update:", msg)
      except Exception:
        pass
  except Exception:
    pass
  finally:
    with workers_lock:
      workers_controlled.pop(worker_uuid, None)
      workers_conns.pop(worker_uuid, None)
    try:
      c.close()
    except Exception:
      pass

def listen_workers():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT + 1))
    s.listen()
    print(f"listening workers on {HOST}:{PORT + 1}")
    while True:
        c, addr = s.accept()
        print(f"receiving connection from WORKER '{addr[0]}:{addr[1]}'")
        threading.Thread(target=receive_alive_worker, daemon=True, args=(c, addr,)).start()

def main():
    check_threshold_thread = threading.Thread(target=check_threshold)
    send_alive_thread = threading.Thread(target=send_alive_master)
    receive_alive_thread = threading.Thread(target=listen_masters)
    listen_workers_thread = threading.Thread(target=listen_workers)

    check_threshold_thread.start()
    send_alive_thread.start()
    receive_alive_thread.start()
    listen_workers_thread.start()

if __name__ == '__main__':
  main()