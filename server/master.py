import socket, threading, json, time, random, logging
from pathlib import Path

# ===================== PATHS, LOG & CONFIG =====================
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "master.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CFG = json.load(f)

HOST        = CFG["server"]["ip"]
PORT        = int(CFG["server"]["port"])
WORKER_PORT = PORT + 1
SERVER_UUID = HOST

PEERS       = CFG.get("peers", [])
HB_INTERVAL = int(CFG["timing"].get("heartbeat_interval", 3))
HB_TIMEOUT  = int(CFG["timing"].get("heartbeat_timeout", 10))
LB_INTERVAL = int(CFG["timing"].get("load_balancer_interval", 2))

QUEUE_THRESHOLD    = int(CFG["load_balancing"].get("threshold_min_tasks", 10))
REQUEST_COOLDOWN   = 10
MIN_KEEP_LOCAL     = int(CFG["load_balancing"].get("min_workers_before_sharing", 1))
IDLE_PING_INTERVAL = 10  # ping quando não há tasks

ENABLE_FAKE_LOAD        = False
FAKE_LOAD_RATE_PER_SEC  = 6

logging.info(f"config carregada: {CONFIG_PATH}")
logging.info(f"Master em {HOST}:{PORT} | Worker port {WORKER_PORT}")
logging.info(f"Peers: {[p['ip'] for p in PEERS]}")
logging.info(f"Threshold fila={QUEUE_THRESHOLD} | HB interval={HB_INTERVAL}s")

# ===================== ESTADO =====================
lock = threading.Lock()
masters_alive     = {}   # {ip: {"last": ts}}
workers_controlled = {}  # {worker_uuid: worker_ip}
worker_conns       = {}  # {worker_uuid: conn}  <-- NOVO: guardamos o socket
borrowed_workers   = {}  # {worker_uuid: {"owner_server_uuid": str, "timestamp": ts}}
task_queue         = []  # lista de timestamps
pending_returns    = {}  # {worker_uuid: borrower_ip}
last_request_time  = 0

# ===================== IO JSON (\n) =====================
def send_json(conn, obj):
    try:
        conn.sendall((json.dumps(obj) + "\n").encode())
        try:
            peer = conn.getpeername()[0]
        except Exception:
            peer = "?"
        logging.info(f"[PAYLOAD→{peer}] {obj}")
    except Exception as e:
        logging.error(f"[send_json] {e}")

def recv_json(conn, timeout=5):
    conn.settimeout(timeout)
    buf = b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                return None
            buf += chunk
            if b"\n" in buf:
                line, _, buf = buf.partition(b"\n")
                try:
                    data = json.loads(line.decode().strip())
                    try:
                        peer = conn.getpeername()[0]
                    except Exception:
                        peer = "?"
                    logging.info(f"[PAYLOAD←{peer}] {data}")
                    return data
                except Exception:
                    return None
    except Exception:
        return None

# ===================== HEARTBEAT =====================
def hb():
    return {"TASK": "HEARTBEAT", "SERVER_UUID": SERVER_UUID}

def hb_ok():
    return {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE", "SERVER_UUID": SERVER_UUID}

def send_alive_master():
    while True:
        for peer in PEERS:
            ip  = peer["ip"]
            port = int(peer.get("port", PORT))

            # não pinga a si mesmo
            if ip == HOST and port == PORT:
                continue

            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
                    sk.settimeout(2)
                    sk.connect((ip, port))
                    send_json(sk, hb())
                    data = recv_json(sk, 2)
                    if data and data.get("RESPONSE") == "ALIVE":
                        with lock:
                            masters_alive[ip] = {"last": time.time()}
                        logging.info(f"[HEARTBEAT] {ip}:{port} vivo")
            except Exception:
                with lock:
                    masters_alive.pop(ip, None)
        time.sleep(HB_INTERVAL)

# ===================== PROTOCOLO MASTER/MASTER =====================
def build_worker_request(needed):
    return {
        "TASK": "WORKER_REQUEST",
        "REQUESTOR_INFO": {
            "ip": HOST,
            "port": WORKER_PORT,
            "needed": int(needed)
        }
    }

def _borrow_plan():
    with lock:
        vivos = [ip for ip in masters_alive.keys() if ip != HOST]
    # para cada master vivo, pede 1 worker
    return [(ip, 1) for ip in vivos]

def ask_for_workers():
    got = False
    for donor_ip, qty in _borrow_plan():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((donor_ip, PORT))
                req = build_worker_request(qty)
                send_json(s, req)
                data = recv_json(s, 3)
                if data and data.get("RESPONSE") == "AVAILABLE":
                    offered = data.get("WORKERS_UUID", [])
                    logging.info(f"[AVAILABLE] {donor_ip} ofereceu {len(offered)}: {offered}")
                    if offered:
                        got = True
        except Exception as e:
            logging.debug(f"[WORKER_REQUEST] {donor_ip} falhou: {e}")
    return got

def initiate_worker_release(owner_server_uuid, workers_list):
    """
    Este master (BURROWER) devolve workers ao DONO:
    1. Fala com o dono via COMMAND_RELEASE.
    2. Depois manda RETURN para cada worker, usando o socket já aberto.
    """
    owner_ip = owner_server_uuid
    try:
        # 1) Fala com o DONO
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((owner_ip, PORT))
            msg = {
                "SERVER_UUID": SERVER_UUID,
                "TASK": "COMMAND_RELEASE",
                "WORKERS_UUID": workers_list
            }
            send_json(s, msg)
            ack = recv_json(s, 5)

        if ack and ack.get("RESPONSE") == "RELEASE_ACK":
            logging.info("[PROTOCOL] RELEASE_ACK recebido. Enviando RETURN aos workers...")
            # 2) Manda RETURN para cada worker usando o socket que JÁ está conectado
            for w in workers_list:
                try:
                    with lock:
                        wconn = worker_conns.get(w)
                    if not wconn:
                        continue
                    ret = {
                        "TASK": "RETURN",
                        "SERVER_RETURN": {
                            "ip": owner_ip,
                            "port": WORKER_PORT
                        }
                    }
                    send_json(wconn, ret)
                    time.sleep(0.2)
                except Exception as e:
                    logging.error(f"[RETURN] {w}: {e}")

            # 3) Limpa estados locais
            with lock:
                for w in workers_list:
                    borrowed_workers.pop(w, None)
                    workers_controlled.pop(w, None)
                    worker_conns.pop(w, None)

    except Exception as e:
        logging.error(f"[COMMAND_RELEASE] erro: {e}")

def send_release_completed(borrower_ip, workers_list):
    """Dono avisa para quem pediu emprestado que os workers voltaram."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((borrower_ip, PORT))
            msg = {
                "SERVER_UUID": SERVER_UUID,
                "RESPONSE": "RELEASE_COMPLETED",
                "WORKERS_UUID": workers_list
            }
            send_json(s, msg)
    except Exception as e:
        logging.error(f"[RELEASE_COMPLETED] erro: {e}")

# ===================== HANDLER MASTER =====================
def receive_master(c, addr):
    try:
        data = recv_json(c, 5)
        if not data:
            return
        task = data.get("TASK")

        if task == "HEARTBEAT":
            with lock:
                masters_alive[addr[0]] = {"last": time.time()}
            send_json(c, hb_ok())

        elif task == "WORKER_REQUEST":
            req    = data.get("REQUESTOR_INFO", {}) or {}
            tip    = req.get("ip")
            tport  = int(req.get("port", WORKER_PORT))
            needed = int(req.get("needed", 1))

            with lock:
                allw      = list(workers_controlled.items())
                lendable  = max(0, len(allw) - MIN_KEEP_LOCAL)
                k         = max(0, min(needed, lendable))
                chosen    = random.sample(allw, k) if k > 0 else []

            if chosen:
                uuids = [u for (u, _ip) in chosen]
                resp  = {
                    "SERVER_UUID": SERVER_UUID,
                    "RESPONSE": "AVAILABLE",
                    "WORKERS_UUID": uuids
                }
                send_json(c, resp)

                # Redireciona CADA worker usando o socket já aberto (worker_conns)
                def _redir():
                    for u, _ in chosen:
                        try:
                            with lock:
                                wconn = worker_conns.get(u)
                            if not wconn:
                                continue
                            cmd = {
                                "TASK": "REDIRECT",
                                "SERVER_REDIRECT": {
                                    "ip": tip,
                                    "port": tport
                                }
                            }
                            send_json(wconn, cmd)
                            time.sleep(0.2)
                        except Exception as e:
                            logging.error(f"[REDIRECT] {u}: {e}")

                threading.Thread(target=_redir, daemon=True).start()

            else:
                resp = {
                    "SERVER_UUID": SERVER_UUID,
                    "RESPONSE": "UNAVAILABLE"
                }
                send_json(c, resp)

        elif task == "COMMAND_RELEASE":
            # Dono recebeu pedido para liberar workers emprestados
            borrower_ip = c.getpeername()[0]
            wlist       = data.get("WORKERS_UUID", [])
            ack         = {
                "RESPONSE": "RELEASE_ACK",
                "SERVER_UUID": SERVER_UUID,
                "WORKERS_UUID": wlist
            }
            send_json(c, ack)
            with lock:
                for w in wlist:
                    pending_returns[w] = borrower_ip

        else:
            if data.get("RESPONSE") == "RELEASE_COMPLETED":
                logging.info(f"[RELEASE_COMPLETED] de {addr[0]} p/ {data.get('WORKERS_UUID')}")
    finally:
        try:
            c.close()
        except:
            pass

# ===================== WORKERS =====================
def receive_alive_worker(conn, addr):
    try:
        data = recv_json(conn, 5)
        if not data:
            conn.close()
            return

        wid         = data.get("WORKER_UUID")
        owner_uuid  = data.get("SERVER_UUID")  # se emprestado

        if not wid:
            logging.warning(f"[WORKER] sem UUID de {addr}")
            conn.close()
            return

        # Guarda IP + conn
        with lock:
            workers_controlled[wid] = addr[0]
            worker_conns[wid]       = conn
            if owner_uuid and owner_uuid != SERVER_UUID:
                borrowed_workers[wid] = {
                    "owner_server_uuid": owner_uuid,
                    "timestamp": time.time()
                }
                logging.info(f"[WORKER] emprestado {wid} de {owner_uuid} conectado de {addr[0]}")
            else:
                logging.info(f"[WORKER] próprio {wid} conectado de {addr[0]}")
                # manda primeira QUERY direto pra demonstrar uso
                send_json(conn, {"TASK": "QUERY", "USER": "11111111111"})

        # Se havia um pending RETURN desse worker, avisa o borrower
        with lock:
            borrower_ip = pending_returns.pop(wid, None)
        if borrower_ip:
            send_release_completed(borrower_ip, [wid])

        threading.Thread(
            target=manage_worker_connection,
            args=(conn, addr, wid),
            daemon=True
        ).start()

    except Exception as e:
        logging.error(f"[receive_alive_worker] {e}")
        try:
            conn.close()
        except:
            pass

def manage_worker_connection(conn, addr, wid):
    """
    Mantém o worker vivo: envia QUERY quando há fila
    e KEEPALIVE quando ocioso.
    """
    last_ping = time.time()
    try:
        conn.settimeout(30)
        while True:
            sent = False

            # 1) se tem tarefa na fila, manda QUERY
            with lock:
                need = bool(task_queue)
                if need:
                    task_queue.pop(0)

            if need:
                try:
                    query = {
                        "TASK": "QUERY",
                        "USER": "11111111111"
                    }
                    send_json(conn, query)
                    sent = True
                except Exception as e:
                    logging.error(f"[QUERY] {wid}: {e}")
                    break

            # 2) se ocioso há X segundos, manda KEEPALIVE
            now = time.time()
            if (not need) and (now - last_ping) >= IDLE_PING_INTERVAL:
                try:
                    keep = {"TASK": "KEEPALIVE"}
                    send_json(conn, keep)
                    sent      = True
                    last_ping = now
                except Exception as e:
                    logging.error(f"[KEEPALIVE] {wid}: {e}")
                    break

            # 3) tenta ler algo de volta, sem travar o loop
            try:
                _ = recv_json(conn, 2)
            except Exception:
                pass

            if not sent:
                time.sleep(0.2)
    finally:
        try:
            conn.close()
        except:
            pass
        with lock:
            workers_controlled.pop(wid, None)
            borrowed_workers.pop(wid, None)
            worker_conns.pop(wid, None)

# ===================== LISTENERS =====================
def listen_masters():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        logging.info(f"[MASTER] escutando {HOST}:{PORT}")
        while True:
            c, addr = s.accept()
            threading.Thread(
                target=receive_master,
                args=(c, addr),
                daemon=True
            ).start()
    except Exception as e:
        logging.error(f"[listen_masters] {e}")

def listen_workers():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, WORKER_PORT))
        s.listen()
        logging.info(f"[WORKER] escutando {HOST}:{WORKER_PORT}")
        while True:
            c, addr = s.accept()
            threading.Thread(
                target=receive_alive_worker,
                args=(c, addr),
                daemon=True
            ).start()
    except Exception as e:
        logging.error(f"[listen_workers] {e}")

# ===================== MONITORES & CARGA =====================
def monitor_request_workers():
    global last_request_time
    while True:
        time.sleep(1)
        with lock:
            qlen = len(task_queue)
            can  = (time.time() - last_request_time) >= REQUEST_COOLDOWN
        if qlen >= QUEUE_THRESHOLD and can:
            if ask_for_workers():
                with lock:
                    last_request_time = time.time()
                logging.info(f"[THRESHOLD] fila={qlen} ≥ {QUEUE_THRESHOLD} → pedido enviado.")

def monitor_release_workers():
    while True:
        time.sleep(LB_INTERVAL)
        with lock:
            q      = len(task_queue)
            blist  = list(borrowed_workers.keys())
            bcount = len(blist)
            owner_uuid = borrowed_workers[blist[0]]["owner_server_uuid"] if bcount else None

        if bcount > 0 and q < QUEUE_THRESHOLD and owner_uuid:
            logging.info(f"[SATURATION] normalizou (fila {q} < {QUEUE_THRESHOLD}). Devolvendo {bcount}...")
            initiate_worker_release(owner_uuid, blist)

def fake_load_generator():
    while True:
        if not ENABLE_FAKE_LOAD:
            time.sleep(1)
            continue
        with lock:
            now = time.time()
            for _ in range(FAKE_LOAD_RATE_PER_SEC):
                task_queue.append(now)
            # mantém só tasks dos últimos 60s
            task_queue[:] = [t for t in task_queue if now - t < 60]
        time.sleep(1)

# ===================== MAIN =====================
def main():
    logging.info("[SYSTEM] Master iniciando...")
    threading.Thread(target=send_alive_master, daemon=True).start()
    threading.Thread(target=listen_masters, daemon=True).start()
    threading.Thread(target=listen_workers, daemon=True).start()
    threading.Thread(target=monitor_request_workers, daemon=True).start()
    threading.Thread(target=monitor_release_workers, daemon=True).start()
    threading.Thread(target=fake_load_generator, daemon=True).start()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()