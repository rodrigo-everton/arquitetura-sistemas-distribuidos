import socket
import threading
import json
import time
import random

HOST = "10.62.217.17"
PORT = 5000

MASTERS = {
    "servers": [
        #{"ip": "10.62.217.199", "name": "joao"},
        #{"ip": "10.62.217.16", "name": "thales.martins"},
        #{"ip": "10.62.217.209", "name": "thiago.machado"},
        #{"ip": "10.62.217.203", "name": "thiago.filho"},
        {"ip": "10.62.217.10", "name": "cassia"}

    ]
}

QUERY_WORKER = {
    "TASK": "QUERY",
    "USER": "11111111111"
}

SEND_ALIVE_MASTER = {"TASK": "HEARTBEAT"}
RESPOND_ALIVE_MASTER = {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
ASK_FOR_WORKERS = {"TASK": "WORKER_REQUEST"}
ASK_FOR_WORKERS_RESPONSE_NEGATIVE = {"RESPONSE": "UNAVAILABLE"}

lock = threading.Lock()
masters_alive = {}
workers_controlled = {}
errorCounter = 0

def send_json(conn, obj):
    data = json.dumps(obj).encode("utf-8") + b"\n"
    conn.sendall(data)

def recv_json(conn):
    raw = conn.recv(4096)
    if not raw:
        return None
    try:
        return json.loads(raw.decode())
    except Exception:
        return None

def send_alive_master():
    while True:
        for server in MASTERS["servers"]:
            ip = server["ip"]
            name = server["name"]
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
                        print(f"[+] Master '{name}' ({ip}) está vivo.")
            except:
                with lock:
                    if name in masters_alive:
                        del masters_alive[name]
                print(f"[-] Falha ao contatar '{name}' ({ip}).")
            time.sleep(1)
        time.sleep(5)

def receive_master(c, addr):
    data = recv_json(c)
    if not data:
        c.close()
        return
    task = data.get("TASK")
    if task == "HEARTBEAT":
        send_json(c, RESPOND_ALIVE_MASTER)
    elif task == "WORKER_REQUEST":
        with lock:
            if workers_controlled:
                uuid, ip = random.choice(list(workers_controlled.items()))
                response = {
                    "RESPONSE": "AVAILABLE",
                    "WORKERS": [{"WORKER_UUID": uuid, "WORKER_IP": ip}],
                    "MSG":"to recebendo"
                }
            else:
                response = ASK_FOR_WORKERS_RESPONSE_NEGATIVE
        send_json(c, response)
    c.close()

def listen_masters():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    print(f"[MASTER] Escutando masters em {HOST}:{PORT}")
    while True:
        c, addr = s.accept()
        threading.Thread(target=receive_master, args=(c, addr), daemon=True).start()

def ask_for_workers():
    with lock:
        alive_list = list(masters_alive.items())
    for name, ip in alive_list:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((ip, PORT))
                print(f"[ASK] Pedindo workers para {name} ({ip})...")
                send_json(s, ASK_FOR_WORKERS)
                data = recv_json(s)
                if not data:
                    continue
                if data.get("RESPONSE") == "AVAILABLE":
                    worker_info = data["WORKERS"][0]
                    uuid = worker_info["WORKER_UUID"]
                    wip = worker_info["WORKER_IP"]
                    with lock:
                        workers_controlled[uuid] = wip
                    print(f"[OK] Recebi worker {uuid} de {name} ({wip})")
                    return True
        except Exception as e:
            print(f"[ERR] Falha ao pedir worker de {name}: {e}")
    return False

def receive_alive_worker(c, addr):
    data = recv_json(c)
    if not data:
        c.close()
        return
    uuid = data.get("WORKER_UUID")
    if uuid:
        with lock:
            workers_controlled[uuid] = addr[0]
        print(f"[WORKER] Registrado {uuid} de {addr[0]}")
    send_json(c, QUERY_WORKER)

def listen_workers():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT + 1))
    s.listen()
    print(f"[WORKER] Escutando workers em {HOST}:{PORT+1}")
    while True:
        c, addr = s.accept()
        threading.Thread(target=receive_alive_worker, args=(c, addr), daemon=True).start()

def monitor_errors():
    global errorCounter
    while True:
        time.sleep(10)
        if errorCounter >= 10:
            print("[!] Muitos erros — pedindo workers adicionais...")
            ask_for_workers()
            errorCounter = 0

def main():
    print("[SYSTEM] Master iniciando...")
    threading.Thread(target=send_alive_master, daemon=True).start()
    threading.Thread(target=listen_masters, daemon=True).start()
    threading.Thread(target=listen_workers, daemon=True).start()
    threading.Thread(target=monitor_errors, daemon=True).start()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
