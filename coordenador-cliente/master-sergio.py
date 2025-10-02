
# master_server.py

import socket
import threading

class MasterServer:
    def __init__(self, host='10.62.217.21', port=9000):
        self.host = host
        self.port = port

    def handle_connection(self, conn, addr):
        print(f"[Master] Conexão recebida de outro master: {addr}")
        while True:
            try:
                data = conn.recv(1024).decode()
                if not data:
                    break
                print(f"[Master] Mensagem do outro master: {data}")
                # Aqui pode responder, se quiser
                conn.sendall(b"ACK")
            except ConnectionResetError:
                print("[Master] Conexão foi encerrada.")
                break
        conn.close()

    def start(self):
        print(f"[Master] Iniciando servidor em {self.host}:{self.port}...")
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()

        print("[Master] Aguardando conexão de outro master...")
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr)).start()