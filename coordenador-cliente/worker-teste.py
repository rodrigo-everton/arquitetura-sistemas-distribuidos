import socket
import json
import threading
import uuid
import time
import random

HOST = "10.62.217.21"  # IP do master (ex: master_a ou master_b)
PORT = 5001             # Porta de comunicação com workers (PORT + 1)

RECONNECT_INTERVAL = 5

def send_json(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode("utf-8"))

def recv_json(conn):
    try:
        raw = conn.recv(4096)
        if not raw:
            return None
        return json.loads(raw.decode())
    except Exception:
        return None

def worker_session(worker_id):
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)
                print(f"[WORKER] Tentando conectar a {HOST}:{PORT}...")
                s.connect((HOST, PORT))

                alive_msg = {"WORKER": "ALIVE", "WORKER_UUID": worker_id}
                send_json(s, alive_msg)

                query = recv_json(s)
                if not query:
                    print("[WORKER] Nenhuma tarefa recebida, aguardando reconexão...")
                    time.sleep(RECONNECT_INTERVAL)
                    continue

                task = query.get("TASK")
                user = query.get("USER")
                print(f"[WORKER] Tarefa recebida: {task} para USER {user}")

                if task == "QUERY":
                    saldo = round(random.uniform(10.0, 1000.0), 2)
                    response = {
                        "WORKER_UUID": worker_id,
                        "CPF": user,
                        "SALDO": saldo,
                        "TASK": "QUERY",
                        "STATUS": "OK"
                    }
                    send_json(s, response)
                    print(f"[WORKER] Enviou saldo R${saldo}")
                else:
                    response = {
                        "WORKER_UUID": worker_id,
                        "TASK": task,
                        "STATUS": "NOK",
                        "ERROR": "Unknown task"
                    }
                    send_json(s, response)
                    print("[WORKER] Tarefa desconhecida recebida")

                time.sleep(RECONNECT_INTERVAL)
        except (ConnectionRefusedError, socket.timeout):
            print(f"[WORKER] Master indisponível ({HOST}:{PORT}), tentando novamente...")
            time.sleep(RECONNECT_INTERVAL)
        except Exception as e:
            print(f"[WORKER] Erro: {e}")
            time.sleep(RECONNECT_INTERVAL)

def main():
    worker_id = str(uuid.uuid4())
    print(f"[WORKER] Iniciado com UUID: {worker_id}")
    worker_thread = threading.Thread(target=worker_session, args=(worker_id,))
    worker_thread.start()
    worker_thread.join()

if __name__ == "__main__":
    main()
