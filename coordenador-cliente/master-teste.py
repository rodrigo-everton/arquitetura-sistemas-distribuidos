import socket
import threading
import json
import time
import random

HOST = "10.62.217.212"
PORT = 5000

MASTERS = {
    "servers": [
        {"ip": "10.62.217.212", "name": "master-meu1"},
        {"ip": "10.62.217.212", "name": "master-meu2"}
    ]
}

QUERY_WORKER = {"TASK": "QUERY", "USER": "11111111111"}
SEND_ALIVE_MASTER = {"TASK": "HEARTBEAT"}
RESPOND_ALIVE_MASTER = {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
ASK_FOR_WORKERS = {"TASK": "WORKER_REQUEST"}
ASK_FOR_WORKERS_RESPONSE_NEGATIVE = {"RESPONSE": "UNAVAILABLE"}

lock = threading.Lock()
masters_alive = {}
workers_controlled = {"uuid-a1": "10.62.217.250"}  # worker fictício
errorCounter = 0

def send_json(conn, obj):
    conn.sendall(json.dumps(obj).encode() + b"\n")

def recv_json(conn):
    try:
        raw = conn.recv(4096)
        if not raw:
            return None
        return json.loads(raw.decode())
    except:
        return None

def send_alive_master():
    while True:
        for server in MASTERS["servers"]:
            ip, name = server["ip"], server["name"]
            if ip == HOST:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect((ip, PORT))
                    send_json(s, SEND_ALIVE_MASTER)
                    data = recv_json(s)
                    if data and data.get("RESPONSE") == "ALIVE":
                        with lock:
                            masters_alive[name] = ip
                        print(f"[+] {name} ({ip}) está vivo")
            except:
                with lock:
                    masters_alive.pop(name, None)
                print(f"[-] Falha ao contatar {name} ({ip})")
            time.sleep(1)
        time.sleep(5)

def receive_master(c, addr):
    data = recv_json(c)
    if not data:
        c.close()
        return
    if data.get("TASK") == "HEARTBEAT":
        send_json(c, RESPOND_ALIVE_MASTER)
    elif data.get("TASK") == "WORKER_REQUEST":
        with lock:
            if workers_controlled:
                uuid, ip = random.choice(list(workers_controlled.items()))
                send_json(c, {"RESPONSE": "AVAILABLE", "WORKERS": [{"WORKER_UUID": uuid, "WORKER_IP": ip}]})
            else:
                send_json(c, ASK_FOR_WORKERS_RESPONSE_NEGATIVE)
    c.close()

def listen_masters():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    print(f"[MASTER A] Escutando em {HOST}:{PORT}")
    while True:
        c, a = s.accept()
        threading.Thread(target=receive_master, args=(c, a), daemon=True).start()

def ask_for_workers():
    with lock:
        alive = list(masters_alive.items())
    for name, ip in alive:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((ip, PORT))
                print(f"[ASK] Pedindo worker para {name} ({ip})...")
                send_json(s, ASK_FOR_WORKERS)
                data = recv_json(s)
                if data and data.get("RESPONSE") == "AVAILABLE":
                    w = data["WORKERS"][0]
                    workers_controlled[w["WORKER_UUID"]] = w["WORKER_IP"]
                    print(f"[OK] Recebi worker {w['WORKER_UUID']} de {name}")
                    return
        except Exception as e:
            print(f"[ERR] Falha ao pedir worker de {name}: {e}")

def monitor_errors():
    global errorCounter
    while True:
        time.sleep(10)
        errorCounter += 1
        if errorCounter >= 3:
            print("[!] Muitos erros — pedindo workers adicionais...")
            ask_for_workers()
            errorCounter = 0

def main():
    threading.Thread(target=send_alive_master, daemon=True).start()
    threading.Thread(target=listen_masters, daemon=True).start()
    threading.Thread(target=monitor_errors, daemon=True).start()
    while True:
        time.sleep(1)