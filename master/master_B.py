import socket
import threading
import uuid
import json

HOST = '0.0.0.0'
PORT = 9001

class ServerB:
    def __init__(self):
        self.server_id = str(uuid.uuid4())
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((HOST, PORT))
        self.sock.listen()
        print(f"[Server B] Ouvindo em {HOST}:{PORT}")

    def handle_client(self, conn, addr):
        print(f"[Server B] Conexão recebida de {addr}")
        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                msg = json.loads(data.decode())
                if msg.get("TASK") == "HEARTBEAT":
                    response = {
                        "SERVER_ID": self.server_id,
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }
                    conn.sendall(json.dumps(response).encode())
                    print(f"[Server B] Respondeu heartbeat para {msg.get('SERVER_ID')}")
            except Exception as e:
                print(f"[Server B] Erro na conexão com {addr}: {e}")
                break
        conn.close()

    def start(self):
        while True:
            conn, addr = self.sock.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()

if __name__ == "__main__":
    server_b = ServerB()
    server_b.start()
