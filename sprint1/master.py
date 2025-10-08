#SPRINT 1 - MASTER / NODE (versão final)
# Este script implementa um nó (master) capaz de:
# - Escutar conexões (servidor)
# - Conectar-se aos outros grupos (cliente)
# - Enviar e responder heartbeats
# - Permitir troca de mensagens de texto entre grupos
# - Detectar peers inativos (timeout)
print("🚀 Iniciando script master.py...", flush=True)
import socket, threading, json, time, uuid, sys

# CONFIGURAÇÕES DOS GRUPOS (fixas)
# GRUPO 1
# IP: 10.62.217.199 | PORT: 8765 | Integrantes: João, Arthur, Carlos
# GRUPO 2 (VOCÊ)
# IP: 10.62.217.212 | PORT: 5900 | Integrantes: Rodrigo, Sérgio, Cássia, Eduardo
# GRUPO 3
# IP: 10.62.217.16  | PORT: 5000 | Integrantes: Davi, Rocha, Pedro, Thales
# GRUPO 4
# IP: 10.62.217.22  | PORT: 5000 | Integrantes: Kawê, André, Thiago Machado
# GRUPO 5
# IP: 10.62.217.203 | PORT: 5000 | Integrante: Guilherme
# =============================================================

# IP e porta do meu GRUPO (2)
HOST = "10.62.217.212"   # para testar local
PORT = 5900

# Intervalos de comunicação
HEARTBEAT_INTERVAL = 5   # Envia heartbeat a cada 5 segundos
TIMEOUT = 15             # Considera offline após 15 segundos sem resposta

# Lista dos outros grupos
KNOWN_PEERS = [
    ("10.62.217.199", 8765),  # Grupo 1
    ("10.62.217.16", 5000),   # Grupo 3
    ("10.62.217.22", 5000),   # Grupo 4
    ("10.62.217.203", 5000),  # Grupo 5
]

class Node:
    def __init__(self, host, port):
        # Gera um UUID único para identificar seu servidor
        self.node_id = str(uuid.uuid4())

        self.host = host
        self.port = port

        # Peers conhecidos (lista de outros grupos)
        self.known_peers = KNOWN_PEERS

        # Tabela de servidores ativos {server_id: último_heartbeat}
        self.peers_alive = {}
        # Lista de sockets abertos (conexões ativas)
        self.peer_sockets = set()
        self.lock = threading.Lock()

    # Função auxiliar para fechar conexões com erro
    def _safe_discard(self, s):
        try:
            s.close()
        except:
            pass
        self.peer_sockets.discard(s)

    # Servidor - aceita conexões de outros grupos
    def server_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        print(f"[{self.node_id[:8]}] Servindo em {self.host}:{self.port}")

        while True:
            conn, addr = srv.accept()
            print(f"[{self.node_id[:8]}] 🔗 Conexão recebida de {addr}")
            with self.lock:
                self.peer_sockets.add(conn)
            threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()

    # Cliente - conecta nos outros grupos
    def connector_loop(self):
        while True:
            for ip, prt in self.known_peers:
                # Evita tentar conectar a si mesmo
                if ip == self.host and prt == self.port:
                    continue
                try:
                    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    conn.settimeout(3)
                    conn.connect((ip, prt))
                    conn.settimeout(None)
                    print(f"[{self.node_id[:8]}] 🔌 Conectado a {ip}:{prt}")
                    with self.lock:
                        self.peer_sockets.add(conn)
                    threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()
                except Exception:
                    pass
            time.sleep(10)

    # Trata mensagens recebidas (heartbeats e mensagens de texto)
    def handle_conn(self, conn):
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                msg = json.loads(data.decode())
                task = msg.get("TASK")
                sender = msg.get("SERVER_ID")

                # Atualiza o último heartbeat do remetente
                if sender:
                    with self.lock:
                        self.peers_alive[sender] = time.time()

                # ---------------- HEARTBEAT ----------------
                if task == "HEARTBEAT":
                    resp = {"SERVER_ID": self.node_id, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                    conn.sendall(json.dumps(resp).encode())

                # ---------------- MENSAGEM -----------------
                elif task == "MESSAGE":
                    content = msg.get("CONTENT", "")
                    print(f"[{self.node_id[:8]}] 💬 Mensagem de {sender[:8]}: {content}")

                # ---------------- RESPOSTA ALIVE -----------
                if msg.get("RESPONSE") == "ALIVE" and sender:
                    print(f"[{self.node_id[:8]}] ✅ ALIVE de {sender[:8]}")

        except Exception:
            pass
        finally:
            with self.lock:
                self._safe_discard(conn)

    # Envia heartbeats contínuos a todos os peers conectados
    def heartbeat_loop(self):
        while True:
            msg = json.dumps({"SERVER_ID": self.node_id, "TASK": "HEARTBEAT"}).encode()
            with self.lock:
                sockets = list(self.peer_sockets)
            for s in sockets:
                try:
                    s.sendall(msg)
                except Exception:
                    with self.lock:
                        self._safe_discard(s)
            time.sleep(HEARTBEAT_INTERVAL)

    # Verifica peers inativos (sem heartbeat no timeout)
    def timeout_loop(self):
        while True:
            now = time.time()
            with self.lock:
                for sid in list(self.peers_alive.keys()):
                    if now - self.peers_alive[sid] > TIMEOUT:
                        print(f"[{self.node_id[:8]}] ⚠️ Peer {sid[:8]} OFFLINE (timeout)")
                        del self.peers_alive[sid]
            time.sleep(HEARTBEAT_INTERVAL)

    # Envia mensagens manuais digitadas pelo usuário
    def send_message(self, text):
        msg = json.dumps({
            "SERVER_ID": self.node_id,
            "TASK": "MESSAGE",
            "CONTENT": text
        }).encode()

        with self.lock:
            sockets = list(self.peer_sockets)
        for s in sockets:
            try:
                s.sendall(msg)
            except Exception:
                with self.lock:
                    self._safe_discard(s)
        print(f"[{self.node_id[:8]}] ✉️ Mensagem enviada.")

    # Lê entrada do teclado e envia para todos os peers conectados
    def input_loop(self):
        print(f"[{self.node_id[:8]}] Digite mensagens e pressione ENTER para enviar (Ctrl+C para sair):")
        while True:
            try:
                text = sys.stdin.readline().strip()
                if text:
                    self.send_message(text)
            except KeyboardInterrupt:
                print("\nEncerrando...")
                break
            except Exception:
                pass
    # Inicializa todas as threads do servidor
    def start(self):
        threading.Thread(target=self.server_loop, daemon=True).start()
        threading.Thread(target=self.connector_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.timeout_loop, daemon=True).start()
        threading.Thread(target=self.input_loop, daemon=True).start()

        while True:
            time.sleep(1)


# Execução principal
if __name__ == "__main__":
    node = Node(HOST, PORT)
    node.start()
