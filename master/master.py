import socket
import threading

HOST = '127.0.0.1'
PORT = 9000

class MasterServer:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.workers = []
        self.lock = threading.Lock()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print(f"Master iniciado em {self.host}:{self.port}. Aguardando Workers...")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Worker conectado de {addr}")
            with self.lock:
                self.workers.append(client_socket)
            threading.Thread(target=self.handle_worker, args=(client_socket,), daemon=True).start()

    def handle_worker(self, worker_socket):
        try:
            while True:
                data = worker_socket.recv(1024)
                if not data:
                    break
                print(f"Recebido do Worker: {data.decode()}")
        except ConnectionResetError:
            pass
        finally:
            print("Worker desconectado.")
            with self.lock:
                self.workers.remove(worker_socket)
            worker_socket.close()
