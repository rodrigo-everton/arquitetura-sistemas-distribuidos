# SPRINT 2 - WORKER (Filho)
# - Gera WORKER_ID (UUID)
# - Conecta ao Pai (endereço fixo do grupo 2)
# - Envia REGISTER e espera REGISTERED
# - Envia HEARTBEAT periódico; espera ALIVE
# - Reconecta se a conexão cair

import socket, json, time, uuid

# Endereço FIXO do PAI (seu grupo 2)
PARENT_HOST = "127.0.0.1"   # "127.0.0.1" para teste local
PARENT_PORT = 5900

# Intervalos
HEARTBEAT_INTERVAL = 5   # envia heartbeat a cada 5s
READ_TIMEOUT      = 5    # aguarda até 5s pelo ALIVE
RECONNECT_DELAY   = 3    # espera 3s antes de tentar reconectar

class Worker:
    def __init__(self):
        # ID único deste worker (fica constante durante a execução)
        self.worker_id = str(uuid.uuid4())
        self.conn = None

    def _close_conn(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self.conn = None

    def connect_parent(self):
        """Conecta ao Pai e realiza o REGISTER com retry infinito."""
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((PARENT_HOST, PARENT_PORT))
                self.conn = s
                print(f"[WORKER {self.worker_id[:8]}] Conectado ao Pai {PARENT_HOST}:{PARENT_PORT}")

                # Envia REGISTER
                reg = {"TASK": "REGISTER", "WORKER_ID": self.worker_id}
                s.sendall(json.dumps(reg).encode())

                # Espera REGISTERED para confirmar registro
                s.settimeout(READ_TIMEOUT)
                data = s.recv(2048)
                s.settimeout(None)

                if not data:
                    raise ConnectionError("Pai encerrou a conexão durante REGISTER")

                msg = json.loads(data.decode())
                if msg.get("TASK") == "REGISTERED" and msg.get("WORKER_ID") == self.worker_id:
                    print(f"[WORKER {self.worker_id[:8]}] Registro confirmado pelo Pai.")
                    return  # sucesso
                else:
                    # Resposta inesperada: fecha e tenta novamente
                    print(f"[WORKER {self.worker_id[:8]}] Resposta inesperada ao REGISTER: {msg}")
                    self._close_conn()
                    time.sleep(RECONNECT_DELAY)

            except Exception as e:
                print(f"[WORKER {self.worker_id[:8]}] Pai indisponível ({e}). Tentando em {RECONNECT_DELAY}s...")
                self._close_conn()
                time.sleep(RECONNECT_DELAY)

    def heartbeat_loop(self):
        """Envia heartbeats e aguarda ALIVE; reconecta se der erro/timeout."""
        while True:
            try:
                hb = {"TASK": "HEARTBEAT", "WORKER_ID": self.worker_id}
                self.conn.sendall(json.dumps(hb).encode())

                # Espera ALIVE do Pai
                self.conn.settimeout(READ_TIMEOUT)
                data = self.conn.recv(2048)
                self.conn.settimeout(None)

                if not data:
                    raise ConnectionError("Pai encerrou a conexão durante HEARTBEAT")

                msg = json.loads(data.decode())
                if msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
                    print(f"[WORKER {self.worker_id[:8]}] Pai respondeu ALIVE.")
                # Qualquer outra mensagem é ignorada no worker (protocolo simples)

            except Exception as e:
                print(f"[WORKER {self.worker_id[:8]}] Conexão perdida ({e}). Reconectando...")
                self._close_conn()
                self.connect_parent()  # bloqueia até registrar de novo

            time.sleep(HEARTBEAT_INTERVAL)

    def start(self):
        print(f"[WORKER {self.worker_id[:8]}] Iniciando… Pai={PARENT_HOST}:{PARENT_PORT}")
        self.connect_parent()
        self.heartbeat_loop()

if __name__ == "__main__":
    Worker().start()
