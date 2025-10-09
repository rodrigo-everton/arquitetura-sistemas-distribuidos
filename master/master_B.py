import socket
import threading
import uuid
import json
import time

HOST = '127.0.0.1'
PORT = 9000
HEARTBEAT_INTERVAL = 5  # segundos
TIMEOUT = 15  # tempo para considerar servidor offline

class ServerA:
    def __init__(self):
        self.server_id = str(uuid.uuid4())
        self.servers_alive = {} 
        self.lock = threading.Lock() # tranca para concorrência do dicionário de servidores entre threads
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((HOST, PORT))
        self.sock.listen()
        print(f"[Server B] Ouvindo em {HOST}:{PORT}")

    def send_heartbeat(self):
        while True:
            msg = json.dumps({
                "SERVER_ID": self.server_id,
                "TASK": "HEARTBEAT"
            }).encode()
            try:
                self.sock.sendall(msg)
                # print("[Server B] Heartbeat enviado")
            except Exception as e:
                print(f"[Server B] Erro ao enviar heartbeat: {e}")
                break
            time.sleep(HEARTBEAT_INTERVAL)

    def listen_responses(self):
        while True:
            try:
                data = self.sock.recv(1024)
                if not data:
                    print("[Server B] Conexão encerrada pelo Server A")
                    break
                msg = json.loads(data.decode())
                if msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
                    server_id = msg.get("SERVER_ID")
                    with self.lock:
                        self.servers_alive[server_id] = time.time()
                    print(f"[Server B] {self.server_id} Recebeu heartbeat ALIVE de {server_id}")
            except Exception as e:
                print(f"[Server B] Erro ao receber dados: {e}")
                break

    def check_timeouts(self):
        while True:
            now = time.time()
            to_remove = []
            with self.lock:
                for server_id, last_time in self.servers_alive.items():
                    if now - last_time > TIMEOUT:
                        print(f"[Server B] Servidor {server_id} considerado offline (timeout)")
                        to_remove.append(server_id)
                for server_id in to_remove:
                    del self.servers_alive[server_id]
                time.sleep(HEARTBEAT_INTERVAL)

    def start(self):
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.listen_responses, daemon=True).start()
        threading.Thread(target=self.check_timeouts, daemon=True).start()

        while True:
            time.sleep(1)  # Mantém o programa rodando


if __name__ == "__main__":
    server_a = ServerA()
    server_a.start()
