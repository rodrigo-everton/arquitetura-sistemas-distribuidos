# worker.py
import argparse
import json
import logging
import os
import socket
import time
import uuid
from pathlib import Path

# ===================== PATHS & LOG =====================
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "worker.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sd.worker")

# ===================== CONFIG HELPERS =====================
def load_master_from_config(path: Path):
    """
    Lê o IP do master e calcula a porta do worker (master_port + 1)
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        ip = cfg["server"]["ip"]
        worker_port = int(cfg["server"]["port"]) + 1
        return ip, worker_port
    except Exception as e:
        logger.warning(f"Não consegui ler {path} ({e}). Usando fallback 127.0.0.1:5001")
        return "127.0.0.1", 5001

# ===================== IO JSON (\n) =====================
def send_json(conn: socket.socket, obj: dict):
    try:
        payload = (json.dumps(obj) + "\n").encode()
        conn.sendall(payload)
        try:
            peer = conn.getpeername()[0]
        except Exception:
            peer = "?"
        logger.info(f"[PAYLOAD→{peer}] {obj}")
    except Exception as e:
        logger.error(f"[send_json] {e}")

def recv_json(conn: socket.socket, timeout: float = 30.0):
    """
    Lê uma única linha JSON finalizada por '\n'.
    """
    conn.settimeout(timeout)
    buf = b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                return None
            buf += chunk
            if b"\n" in buf:
                line, _, _rest = buf.partition(b"\n")
                try:
                    data = json.loads(line.decode().strip())
                    try:
                        peer = conn.getpeername()[0]
                    except Exception:
                        peer = "?"
                    logger.info(f"[PAYLOAD←{peer}] {data}")
                    return data
                except Exception:
                    return None
    except Exception:
        return None

# ===================== WORKER LOOP =====================
def connect_and_run(current_master_ip: str, worker_port: int, worker_uuid: str, master_origin: str | None):
    """
    Conecta no master, registra-se, processa comandos até receber REDIRECT/RETURN/desconectar.
    Retorna: (action, target_ip, target_port, new_origin)
      action ∈ {"REDIRECT","RETURN","DISCONNECT"}
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)
        s.connect((current_master_ip, worker_port))

        # Mensagem de ALIVE
        alive = {"WORKER": "ALIVE", "WORKER_UUID": worker_uuid}
        # Se estou sendo emprestado, informo quem é o dono original
        if master_origin and master_origin != current_master_ip:
            alive["SERVER_UUID"] = master_origin
        send_json(s, alive)

        while True:
            msg = recv_json(s, 30)
            if not msg:
                break

            task = msg.get("TASK")

            if task == "REDIRECT":
                # { "TASK": "REDIRECT", "SERVER_REDIRECT": {"ip": "...", "port": 5001} }
                target = msg.get("SERVER_REDIRECT", {}) or {}
                tip = target.get("ip")
                tport = int(target.get("port", worker_port))
                if tip:
                    new_origin = master_origin or current_master_ip
                    logger.info(f"[REDIRECT] indo para {tip}:{tport} (origin={new_origin})")
                    return ("REDIRECT", tip, tport, new_origin)

            elif task == "RETURN":
                # { "TASK": "RETURN", "SERVER_RETURN": {"ip": "...", "port": 5001} }
                target = msg.get("SERVER_RETURN", {}) or {}
                tip = target.get("ip")
                tport = int(target.get("port", worker_port))
                if tip:
                    logger.info(f"[RETURN] voltando ao dono {tip}:{tport}")
                    return ("RETURN", tip, tport, None)

            elif task == "QUERY":
                # Simula processamento e responde OK
                time.sleep(1)
                resp = {
                    "WORKER_UUID": worker_uuid,
                    "TASK": "QUERY",
                    "STATUS": "OK",
                    "CPF": "11111111111",
                    "SALDO": "25.000"
                }
                send_json(s, resp)

            else:
                # Resposta genérica NOK para qualquer comando desconhecido
                time.sleep(1)
                resp = {
                    "WORKER": "EDUARDO",
                    "TASK": "QUERY",
                    "STATUS": "NOK",
                    "ERROR": "User not found",
                    "CPF": "11111111111",
                    "SALDO": 0
                }
                send_json(s, resp)

    return ("DISCONNECT", None, None, None)

# ===================== MAIN =====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker simples do SD")
    parser.add_argument("--master-ip", help="IP do master (se não passar, lê do config.json)")
    parser.add_argument("--worker-port", type=int, help="Porta do master para workers (padrão: master_port+1 do config)")
    parser.add_argument("--uuid", help="UUID fixo para o worker (padrão: geração randômica)")
    args = parser.parse_args()

    # UUID do worker
    WORKER_UUID = args.uuid or str(uuid.uuid4())

    # Descobre IP/porta
    ip_cfg, port_cfg = load_master_from_config(CONFIG_PATH)
    current_master = args.master_ip or ip_cfg
    WORKER_PORT = args.worker_port or port_cfg

    master_origin: str | None = None  # preenchido quando for emprestado

    logger.info(f"Worker UUID = {WORKER_UUID}")
    logger.info(f"Conectando no master {current_master}:{WORKER_PORT}")

    try:
        while True:
            action, tip, tport, new_origin = connect_and_run(current_master, WORKER_PORT, WORKER_UUID, master_origin)

            if action in ("REDIRECT", "RETURN"):
                # Atualiza origem quando for emprestado; limpa quando retorna
                if action == "REDIRECT" and new_origin:
                    master_origin = new_origin
                elif action == "RETURN":
                    master_origin = None

                current_master = tip
                WORKER_PORT = tport
                time.sleep(0.5)
                continue

            # DISCONNECT → tenta reconectar no atual
            logger.warning("Desconectado. Reconnecting in 2s...")
            time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Worker encerrado pelo usuário.")
