import socket
import threading
import json
import time
import random

HOST = "192.168.15.4"
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
    try:
        data = json.dumps(obj).encode("utf-8") + b"\n"
        conn.sendall(data)
    except Exception as e:
        print(f"[ERROR] send json error: {e}")

def recv_json(conn):
    try:
        raw = conn.recv(4096)
        if not raw:
            return None
        try:
            return json.loads(raw.decode())
        except Exception:
            return None
    except Exception as e:
        print(f"[ERROR] recv json error: {e}")
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
            except socket.timeout:
                with lock:
                    if name in masters_alive:
                        del masters_alive[name]
                print(f"[-] Timeout ao contatar '{name}' ({ip}).")
            except socket.error as e:
                with lock:
                    if name in masters_alive:
                        del masters_alive[name]
                print(f"[-] Falha ao contatar '{name}' ({ip}): {e}")
            except Exception as e:
                print(f"[-] Erro inesperado em send_alive_master: {e}")
            time.sleep(1)
        time.sleep(5)

def receive_master(c, addr):
    try:
        data = recv_json(c)
        if not data:
            c.close()
            return
        try:
            task = data.get("TASK")
        except (TypeError, AttributeError) as e:
            print(f"[-] falha ao obter TASK de {addr}: {e}")
            c.close()
            return
        if task == "HEARTBEAT":
            try:
                send_json(c, RESPOND_ALIVE_MASTER)
            except Exception as e:
                print(f"[-] heartbeat response error {addr}: {e}")
        elif task == "WORKER_REQUEST":
            try:
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
                try:
                    send_json(c, response)
                except Exception as e:
                    print(f"[-] worker request send error {addr}: {e}")
            except Exception as e:
                print(f"[-] worker request response error {addr}: {e}")
    except Exception as e:
        print(f"[-] unexpected receive_master error {addr}: {e}")
    finally:
        c.close()

def listen_masters():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"[MASTER] Escutando masters em {HOST}:{PORT}")
        while True:
            try:
                c, addr = s.accept()
                threading.Thread(target=receive_master, args=(c, addr), daemon=True).start()

            except socket.error as e:
                print(f"[ERROR] master accept socket error: {e}")
            except Exception as e:
                print(f"[ERROR] unexpected thread start error: {e}")
    except socket.error as e:
        print(f"[ERROR] master listening socket error: {e}")
    except Exception as e:
        print(f"[ERROR] unexpected listen_masters setup error: {e}")

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
                try:
                    if data.get("RESPONSE") == "AVAILABLE":
                        worker_info = data["WORKERS"][0]
                        uuid = worker_info["WORKER_UUID"]
                        wip = worker_info["WORKER_IP"]
                        with lock:
                            workers_controlled[uuid] = wip
                        print(f"[OK] Recebi worker {uuid} de {name} ({wip})")
                        return True
                except (KeyError, IndexError, TypeError) as e:
                    print(f"[ERROR] falha no parse do ask_for_workers {e}")
        except socket.timeout:
            print(f"[TIMEOUT] Timeout ao pedir worker de {name}: timeout")
        except socket.error as e:
            print(f"[ERROR] Falha ao pedir worker de {name}: {e}")
        except Exception as e:
            print(f"[ERROR] Erro inesperado com {name}: {e}")
    return False

def manage_worker_connection(conn, addr, uuid):
    try:
        conn.settimeout(30)
        
        while True:
            try:
                data = recv_json(conn)
                if not data:
                    print(f"[!] Worker {uuid} desconectou")
                    break
                try:
                    send_json(conn, QUERY_WORKER)
                except Exception as e:
                    print(f"[ERROR] Falha ao enviar QUERY para worker {uuid}: {e}")
                    break
            
            except socket.timeout:
                print(f"[TIMEOUT] Worker {uuid} não respondeu")
                break
            except Exception as e:
                print(f"[ERROR] Worker {uuid}: {e}")
                break
    except Exception as e:
        print(f"[ERROR] erro no setup da conexão com o worker {uuid}: {e}")
    finally:
        conn.close()
        with lock:
            if uuid in workers_controlled:
                del workers_controlled[uuid]

def receive_alive_worker(conn, addr):
    try:
        data = recv_json(conn)
        if not data:
            conn.close()
            return
        try:
            uuid = data.get("WORKER_UUID")
        except (TypeError, AttributeError) as e:
            print(f"[ERROR] falha ao obter WORKER_UUID de {addr}: {e}")
        if uuid:
            with lock:
                workers_controlled[uuid] = addr[0]
            print(f"[WORKER] Registrado {uuid} de {addr[0]}")
            
            send_json(conn, QUERY_WORKER)
            
            threading.Thread(
                target=manage_worker_connection,
                args=(conn, addr, uuid),
                daemon=True
            ).start()
        else:
            conn.close()
    except Exception as e:
        print(f"[ERROR] Falha ao registrar worker de {addr}: {e}")
        pass

def listen_workers():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT + 1))
        s.listen()
        print(f"[WORKER] Escutando workers em {HOST}:{PORT+1}")
        while True:
            try:
                c, addr = s.accept()
                threading.Thread(target=receive_alive_worker, args=(c, addr), daemon=True).start()
            except socket.error as e:
                print(f"[ERROR] erro de socket accept na escuta de workers: {e}")
            except Exception as e:
                print(f"[ERROR] erro inesperado no start da thread de tasks para workers: {e}")
    except socket.error as e:
        print(f"[ERROR] erro de socket na escuta de workers: {e}")
    except Exception as e:
        print(f"[ERROR] erro inesperado na configuração da escuta de workers: {e}")

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
