import socket
import threading
import json
import time
import random
import logging
import uuid
import os
import psutil
import platform
from datetime import datetime
from pathlib import Path

#PATHS, LOG & CONFIG
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

SERVER_UUID = f"SERVER_{CFG['server'].get('id_number', 0)}"

PEERS       = CFG.get("peers", [])
HB_INTERVAL = int(CFG["timing"].get("heartbeat_interval", 3))
HB_TIMEOUT  = int(CFG["timing"].get("heartbeat_timeout", 10))
LB_INTERVAL = int(CFG["timing"].get("load_balancer_interval", 2))

QUEUE_THRESHOLD    = int(CFG["load_balancing"].get("threshold_min_tasks", 10))
REQUEST_COOLDOWN   = 10
MIN_KEEP_LOCAL     = int(CFG["load_balancing"].get("min_workers_before_sharing", 1))
IDLE_PING_INTERVAL = 10

ENABLE_FAKE_LOAD       = False
FAKE_LOAD_RATE_PER_SEC = 2

#SUPERVISOR (SPRINT 5)
SUP_HOST         = "10.62.217.32"
SUP_PORT         = 8000
METRICS_INTERVAL = 10
START_TIME       = time.time()

logging.info(f"Master {SERVER_UUID} em {HOST}:{PORT} | Worker port {WORKER_PORT}")
logging.info(f"Peers: {[p['ip'] for p in PEERS]}")
logging.info(f"Threshold fila={QUEUE_THRESHOLD} | HB interval={HB_INTERVAL}s")

#ESTADO
lock = threading.Lock()
masters_alive      = {}   # {ip: {"last": ts}}
workers_controlled = {}   # {worker_uuid: worker_ip}
worker_conns       = {}   # {worker_uuid: conn}
borrowed_workers   = {}   # {worker_uuid: {"owner_server_uuid": str, "timestamp": ts}}
pending_returns    = {}   # {worker_uuid: borrower_ip}
task_queue         = []   # lista de timestamps
last_request_time  = 0

#IO JSON
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

#HEARTBEAT
def hb():
    return {"SERVER_UUID": SERVER_UUID, "TASK": "HEARTBEAT"}

def hb_ok():
    return {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE", "SERVER_UUID": SERVER_UUID}

def send_alive_master():
    while True:
        for peer in PEERS:
            ip   = peer["ip"]
            port = int(peer.get("port", PORT))

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

#PROTOCOLO MASTER/MASTER
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
    Este master (BORROWER) devolve workers ao DONO:
    1. Fala com o dono via COMMAND_RELEASE.
    2. Depois manda RETURN para cada worker.
    """
    owner_ip = owner_server_uuid  #o id do dono é o IP do servidor
    try:
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

            with lock:
                for w in workers_list:
                    borrowed_workers.pop(w, None)
                    workers_controlled.pop(w, None)
                    worker_conns.pop(w, None)

    except Exception as e:
        logging.error(f"[COMMAND_RELEASE] erro: {e}")

def send_release_completed(borrower_ip, workers_list):
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
        except Exception:
            pass

#WORKERS
def receive_alive_worker(conn, addr):
    try:
        data = recv_json(conn, 5)
        if not data:
            conn.close()
            return

        wid         = data.get("WORKER_UUID")
        owner_uuid  = data.get("SERVER_UUID")

        if not wid:
            logging.warning(f"[WORKER] sem UUID de {addr}")
            conn.close()
            return

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
                send_json(conn, {"TASK": "QUERY", "USER": "11111111111"})

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
        except Exception:
            pass

def manage_worker_connection(conn, addr, wid):
    last_ping = time.time()
    try:
        conn.settimeout(30)
        while True:
            sent = False

            with lock:
                need = bool(task_queue)
                if need:
                    task_queue.pop(0)

            if need:
                try:
                    query = {"TASK": "QUERY", "USER": "11111111111"}
                    send_json(conn, query)
                    sent = True
                except Exception as e:
                    logging.error(f"[QUERY] {wid}: {e}")
                    break

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

            try:
                _ = recv_json(conn, 2)
            except Exception:
                pass

            if not sent:
                time.sleep(0.2)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        with lock:
            workers_controlled.pop(wid, None)
            borrowed_workers.pop(wid, None)
            worker_conns.pop(wid, None)

#LISTENERS
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

#MONITORES & CARGA
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
            task_queue[:] = [t for t in task_queue if now - t < 60]
        time.sleep(1)

#MÉTRICAS PARA SUPERVISOR
def iso_now(ts=None):
    if ts is None:
        dt = datetime.utcnow()
    else:
        dt = datetime.utcfromtimestamp(ts)
    return dt.isoformat(timespec="seconds") + "Z"

def build_neighbors_state():
    neigh = []
    with lock:
        alive_copy = dict(masters_alive)
    for peer in PEERS:
        sid = peer.get("id")
        ip  = peer.get("ip")
        if not sid or not ip:
            continue
        info = alive_copy.get(ip)
        if info:
            status  = "available"
            last_hb = iso_now(info["last"])
        else:
            status  = "unavailable"
            last_hb = None
        neigh.append({
            "server_uuid": sid,
            "status": status,
            "last_heartbeat": last_hb
        })
    return neigh

def build_farm_state():
    with lock:
        total_workers    = len(workers_controlled)
        received_workers = len(borrowed_workers)
        tasks_pending    = len(task_queue)

        workers_utilization = min(total_workers, tasks_pending)
        workers_idle        = max(0, total_workers - workers_utilization)
        workers_borrowed    = received_workers
        workers_failed      = 0
        tasks_running       = workers_utilization

    workers_state = {
        "total_registered": total_workers,
        "workers_utilization": workers_utilization,
        "workers_alive": workers_utilization + received_workers,
        "workers_idle": workers_idle,
        "workers_borrowed": workers_borrowed,
        "workers_recieved": received_workers,
        "workers_failed": workers_failed
    }
    tasks_state = {
        "tasks_pending": tasks_pending,
        "tasks_running": tasks_running
    }
    return workers_state, tasks_state

def build_system_state():
    uptime_seconds = int(time.time() - START_TIME)

    # LOAD AVERAGE
    if platform.system() == "Windows":
        load1 = psutil.cpu_percent(interval=0.3) / 10
        load5 = psutil.cpu_percent(interval=0.1) / 15
    else:
        load1, load5, _ = os.getloadavg()

    # CPU real
    cpu_usage_percent  = psutil.cpu_percent(interval=0.2)
    cpu_count_logical  = psutil.cpu_count(logical=True)
    cpu_count_physical = psutil.cpu_count(logical=False)

    # Memória real
    mem = psutil.virtual_memory()
    total_mb     = round(mem.total / (1024 * 1024), 1)
    available_mb = round(mem.available / (1024 * 1024), 1)
    percent_used = mem.percent
    memory_used  = round(total_mb - available_mb, 1)

    # Disco real
    if platform.system() == "Windows":
        disk = psutil.disk_usage("C:\\")
    else:
        disk = psutil.disk_usage("/")

    disk_total_gb = round(disk.total / (1024**3), 1)
    disk_free_gb  = round(disk.free  / (1024**3), 1)
    disk_percent  = disk.percent

    return {
        "uptime_seconds": uptime_seconds,
        "load_average_1m": round(load1, 2),
        "load_average_5m": round(load5, 2),
        "cpu": {
            "usage_percent": round(cpu_usage_percent, 2),
            "count_logical": cpu_count_logical,
            "count_physical": cpu_count_physical
        },
        "memory": {
            "total_mb": total_mb,
            "available_mb": available_mb,
            "percent_used": percent_used,
            "memory_used": memory_used
        },
        "disk": {
            "total_gb": disk_total_gb,
            "free_gb": disk_free_gb,
            "percent_used": disk_percent
        }
    }

def build_performance_payload():
    system_state = build_system_state()
    workers_state, tasks_state = build_farm_state()
    neighbors = build_neighbors_state()

    payload = {
        "server_uuid": SERVER_UUID,
        "task": "performance_report",
        "timestamp": iso_now(),
        "mensage_id": str(uuid.uuid4()),
        "performance": {
            "system": system_state,
            "farm_state": {
                "workers": workers_state,
                "tasks": tasks_state
            },
            "config_thresholds": {
                "max_task": QUEUE_THRESHOLD
            },
            "neighbors": neighbors
        }
    }
    return payload

def send_metrics_to_supervisor():
    while True:
        try:
            payload = build_performance_payload()
            logging.info(f"[SUPERVISOR] enviando métricas para {SUP_HOST}:{SUP_PORT}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((SUP_HOST, SUP_PORT))
                s.sendall((json.dumps(payload) + "\n").encode())
        except Exception as e:
            logging.error(f"[SUPERVISOR] erro ao enviar métricas: {e}")
        time.sleep(METRICS_INTERVAL)

def main():
    logging.info("[SYSTEM] Master iniciando...")
    threading.Thread(target=send_alive_master,        daemon=True).start()
    threading.Thread(target=listen_masters,           daemon=True).start()
    threading.Thread(target=listen_workers,           daemon=True).start()
    threading.Thread(target=monitor_request_workers,  daemon=True).start()
    threading.Thread(target=monitor_release_workers,  daemon=True).start()
    threading.Thread(target=fake_load_generator,      daemon=True).start()
    threading.Thread(target=send_metrics_to_supervisor, daemon=True).start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
