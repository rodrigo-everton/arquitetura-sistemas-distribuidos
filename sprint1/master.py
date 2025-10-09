# SPRINT 1 + 2 - MASTER/PAI (versão unificada)
# Este script:
#  - SPRINT 1: master ↔ masters (heartbeat + mensagens)
#  - SPRINT 2: PAI ↔ workers (REGISTER + heartbeat + timeout)
print("🚀 Iniciando script master.py...", flush=True)

import socket, threading, json, time, uuid, sys

# ================== CONFIGURAÇÕES FIXAS DOS GRUPOS ==================
# GRUPO 1 -> 10.62.217.199:8765
# GRUPO 2 -> 10.62.217.212:5900  (SEU GRUPO)
# GRUPO 3 -> 10.62.217.16 :5000
# GRUPO 4 -> 10.62.217.22 :5000
# GRUPO 5 -> 10.62.217.203:5000
# ====================================================================

# 👉 IP e Porta do SEU GRUPO (Grupo 2)
HOST = "10.62.217.212"   # para testar local você pode usar "0.0.0.0" ou "127.0.0.1"
PORT = 5900

# Intervalos de comunicação
HEARTBEAT_INTERVAL = 5   # envia/checa a cada 5s
TIMEOUT = 15             # considera offline após 15s sem sinal

# Peers (outros masters) — SPRINT 1
KNOWN_PEERS = [
    ("10.62.217.199", 8765),  # Grupo 1
    ("10.62.217.16",  5000),  # Grupo 3
    ("10.62.217.22",  5000),  # Grupo 4
    ("10.62.217.203", 5000),  # Grupo 5
]

class Node:
    def __init__(self, host, port):
        # ID único deste master/pai
        self.node_id = str(uuid.uuid4())

        # Endereço do SEU grupo
        self.host = host
        self.port = port

        # ---- Sprint 1 (masters) ----
        self.known_peers = KNOWN_PEERS                 # lista de (ip, porta) de outros masters
        self.peers_alive = {}                          # {server_id: last_seen}
        self.peer_sockets = set()                      # sockets de MASTERS apenas

        # ---- Sprint 2 (workers) ----
        self.workers = {}                               # {worker_id: {"conn": socket, "last_seen": ts}}

        # Sincronização
        self.lock = threading.Lock()

    # ---------- util ----------
    def _safe_discard(self, s):
        try: s.close()
        except: pass
        self.peer_sockets.discard(s)

    # ---------- servidor: aceita conexões (masters OU workers) ----------
    def server_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        print(f"[{self.node_id[:8]}] Servindo em {self.host}:{self.port}")

        while True:
            conn, addr = srv.accept()
            print(f"[{self.node_id[:8]}] 🔗 Conexão recebida de {addr}")
            # Por padrão ainda não sabemos se é master ou worker.
            # Vamos adicionar temporariamente, e ao identificar worker removeremos
            with self.lock:
                self.peer_sockets.add(conn)
            threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()

    # ---------- cliente: conecta nos outros masters ----------
    def connector_loop(self):
        while True:
            for ip, prt in self.known_peers:
                # evita conectar em si mesmo
                if ip == self.host and prt == self.port:
                    continue
                try:
                    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    conn.settimeout(3)
                    conn.connect((ip, prt))
                    conn.settimeout(None)
                    print(f"[{self.node_id[:8]}] 🔌 Conectado a {ip}:{prt}")
                    with self.lock:
                        self.peer_sockets.add(conn)  # isto é um MASTER
                    threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()
                except Exception:
                    pass
            time.sleep(10)

    # ---------- tratamento de mensagens ----------
    def handle_conn(self, conn):
        # Se este socket pertencer a um worker, guardaremos seu id aqui:
        current_worker_id = None
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                try:
                    msg = json.loads(data.decode())
                except json.JSONDecodeError:
                    continue

                task   = msg.get("TASK")
                sender = msg.get("SERVER_ID")   # usado por outros masters (Sprint 1)
                w_id   = msg.get("WORKER_ID")   # usado pelos workers (Sprint 2)

                # ======== SPRINT 1: mensagens entre MASTERS ========
                if sender:
                    # marca que o master "sender" está vivo
                    with self.lock:
                        self.peers_alive[sender] = time.time()

                if task == "HEARTBEAT" and sender:
                    # resposta ao heartbeat de outro master
                    resp = {"SERVER_ID": self.node_id, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                    conn.sendall(json.dumps(resp).encode())

                if msg.get("RESPONSE") == "ALIVE" and sender:
                    print(f"[{self.node_id[:8]}] ✅ ALIVE de {sender[:8]}")

                # ======== SPRINT 2: mensagens de WORKERS ========
                if task == "REGISTER" and w_id:
                    current_worker_id = w_id
                    with self.lock:
                        # identifica como worker: NÃO queremos enviar heartbeat para ele,
                        # então removemos esta conexão do conjunto de peers (masters)
                        self.peer_sockets.discard(conn)
                        self.workers[w_id] = {"conn": conn, "last_seen": time.time()}
                    # confirma registro
                    resp = {"TASK": "REGISTERED", "WORKER_ID": w_id}
                    conn.sendall(json.dumps(resp).encode())
                    print(f"[{self.node_id[:8]}] 👷 Worker registrado: {w_id[:8]}")

                elif task == "HEARTBEAT" and w_id:
                    current_worker_id = w_id
                    with self.lock:
                        # marca como worker e atualiza last_seen
                        self.peer_sockets.discard(conn)  # garante que não receberá nossos heartbeats
                        if w_id in self.workers:
                            self.workers[w_id]["last_seen"] = time.time()
                        else:
                            self.workers[w_id] = {"conn": conn, "last_seen": time.time()}
                    # responde ALIVE para o worker
                    resp = {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                    conn.sendall(json.dumps(resp).encode())

                # Mensagem texto entre masters (opcional)
                if task == "MESSAGE" and sender:
                    content = msg.get("CONTENT", "")
                    print(f"[{self.node_id[:8]}] 💬 Msg de master {sender[:8]}: {content}")

        except Exception:
            pass
        finally:
            # Limpeza ao encerrar a conexão
            with self.lock:
                self._safe_discard(conn)
                # Se este socket era de um worker registrado, remove-o
                if current_worker_id and current_worker_id in self.workers:
                    # confere se o conn armazenado é este
                    try:
                        if self.workers[current_worker_id]["conn"] is conn:
                            del self.workers[current_worker_id]
                            print(f"[{self.node_id[:8]}] 🗑️ Worker {current_worker_id[:8]} removido (desconexão)")
                    except:
                        pass

    # ---------- envia heartbeats (APENAS para masters) ----------
    def heartbeat_loop(self):
        while True:
            payload = json.dumps({"SERVER_ID": self.node_id, "TASK": "HEARTBEAT"}).encode()
            with self.lock:
                sockets = list(self.peer_sockets)  # aqui só ficam MASTERS
            for s in sockets:
                try:
                    s.sendall(payload)
                except Exception:
                    with self.lock:
                        self._safe_discard(s)
            time.sleep(HEARTBEAT_INTERVAL)

    # ---------- timeout de masters e workers ----------
    def timeout_loop(self):
        while True:
            now = time.time()
            with self.lock:
                # masters (Sprint 1)
                for sid in list(self.peers_alive.keys()):
                    if now - self.peers_alive[sid] > TIMEOUT:
                        print(f"[{self.node_id[:8]}] ⚠️ Master {sid[:8]} OFFLINE (timeout)")
                        del self.peers_alive[sid]
                # workers (Sprint 2)
                for wid in list(self.workers.keys()):
                    if now - self.workers[wid]["last_seen"] > TIMEOUT:
                        try:
                            self.workers[wid]["conn"].close()
                        except:
                            pass
                        del self.workers[wid]
                        print(f"[{self.node_id[:8]}] ⚠️ Worker {wid[:8]} OFFLINE (timeout)")
            time.sleep(HEARTBEAT_INTERVAL)

    # ---------- envio manual de mensagem (apenas para masters conectados) ----------
    def send_message(self, text):
        msg = json.dumps({
            "SERVER_ID": self.node_id,
            "TASK": "MESSAGE",
            "CONTENT": text
        }).encode()
        with self.lock:
            sockets = list(self.peer_sockets)  # só masters
        for s in sockets:
            try:
                s.sendall(msg)
            except Exception:
                with self.lock:
                    self._safe_discard(s)
        print(f"[{self.node_id[:8]}] ✉️ Mensagem enviada a masters conectados.")

    # ---------- input de linha de comando ----------
    def input_loop(self):
        print(f"[{self.node_id[:8]}] Digite mensagens p/ outros masters e ENTER (Ctrl+C p/ sair):")
        print(f"[{self.node_id[:8]}] Comandos: /workers  -> lista workers vivos")
        while True:
            try:
                text = sys.stdin.readline().strip()
                if not text:
                    continue
                if text == "/workers":
                    with self.lock:
                        vivos = list(self.workers.keys())
                    if vivos:
                        print(f"[{self.node_id[:8]}] Workers vivos: " + ", ".join(w[:8] for w in vivos))
                    else:
                        print(f"[{self.node_id[:8]}] Nenhum worker vivo no momento.")
                else:
                    self.send_message(text)
            except KeyboardInterrupt:
                print("\nEncerrando...")
                break
            except Exception:
                pass

    # ---------- start ----------
    def start(self):
        threading.Thread(target=self.server_loop,    daemon=True).start()
        threading.Thread(target=self.connector_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.timeout_loop,   daemon=True).start()
        threading.Thread(target=self.input_loop,     daemon=True).start()
        while True:
            time.sleep(1)

# ---------- main ----------
if __name__ == "__main__":
    node = Node(HOST, PORT)
    node.start()
