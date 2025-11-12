import socket, threading, json, time, random, logging

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

# ===================== CONFIG POR GRUPO =====================
HOST = "127.0.0.1"         
PORT = 5000                    # porta masters
WORKER_PORT = PORT + 1         # porta workers
SERVER_UUID = "id1"             # simples p/ demo: UUID == IP

MASTERS = {                    # igual em todos
    "servers": [
          {"ip":"127.0.0.1","name":"id1","port":5000},
          {"ip":"127.0.0.1","name":"id2","port":5100},
          {"ip":"127.0.0.1","name":"id3","port":5200},
          {"ip":"127.0.0.1","name":"id4","port":5300}
    ]
}

# plano opcional: quem empresta quanto (para id1 saturado, por ex.)
BORROW_AMOUNTS = {
    # "10.62.217.212": 1,  # id2 empresta 1
    # "10.62.217.13":  2,  # id3 empresta 2
    # "10.62.217.209": 1,  # id4 empresta 1
}

MIN_KEEP_LOCAL   = 1  # cada doador mantém >=1 worker local
QUEUE_THRESHOLD  = 10 # RN oficial (fila >=10 pede ajuda; <10 devolve)
REQUEST_COOLDOWN = 10 # anti-flood de pedidos
last_request_time = 0

# (opcional) carga fake para demonstração — ligue só em quem vai saturar
ENABLE_FAKE_LOAD = False
FAKE_LOAD_RATE_PER_SEC = 6

# ===================== ESTADO =====================
lock = threading.Lock()
masters_alive = {}            # {ip: {"last": ts}}
workers_controlled = {}       # {worker_uuid: worker_ip}
borrowed_workers = {}         # {worker_uuid: {"owner_server_uuid": str, "timestamp": ts}}
task_queue = []               # lista de timestamps
pending_returns = {}          # {worker_uuid: borrower_ip}

# ===================== IO JSON (\n) =====================
def send_json(conn, obj):
    try:
        conn.sendall((json.dumps(obj) + "\n").encode())
    except Exception as e:
        logging.error(f"[send_json] {e}")

def recv_json(conn, timeout=5):
    conn.settimeout(timeout)
    try:
        raw = conn.recv(4096)
        if not raw: return None
        try: return json.loads(raw.decode().strip())
        except Exception: return None
    except Exception:
        return None

# ===================== HEARTBEAT =====================
def hb():  return {"TASK": "HEARTBEAT", "SERVER_UUID": SERVER_UUID}
def hb_ok(): return {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE", "SERVER_UUID": SERVER_UUID}

def send_alive_master():
    while True:
        for s in MASTERS["servers"]:
            ip = s["ip"]
            if ip == HOST: continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
                    sk.settimeout(2); sk.connect((ip, PORT))
                    send_json(sk, hb()); logging.info(f"[PAYLOAD→{ip}] {hb()}")
                    data = recv_json(sk, 2)
                    if data and data.get("RESPONSE") == "ALIVE":
                        with lock: masters_alive[ip] = {"last": time.time()}
                        logging.info(f"[HEARTBEAT] {ip} vivo")
            except Exception:
                with lock: masters_alive.pop(ip, None)
        time.sleep(5)

# ===================== PROTOCOLO MASTER/MASTER =====================
def build_worker_request(needed):
    return {"TASK": "WORKER_REQUEST",
            "REQUESTOR_INFO": {"ip": HOST, "port": WORKER_PORT, "needed": int(needed)}}

def _borrow_plan():
    with lock: vivos = [ip for ip in masters_alive.keys() if ip != HOST]
    if BORROW_AMOUNTS:
        return [(ip, BORROW_AMOUNTS[ip]) for ip in BORROW_AMOUNTS if ip in vivos and BORROW_AMOUNTS[ip] > 0]
    return [(ip, 1) for ip in vivos]  # fallback: 1 de cada

def ask_for_workers():
    got = False
    for donor_ip, qty in _borrow_plan():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3); s.connect((donor_ip, PORT))
                req = build_worker_request(qty)
                send_json(s, req); logging.info(f"[PAYLOAD→{donor_ip}] {req}")
                data = recv_json(s, 3)
                if data and data.get("RESPONSE") == "AVAILABLE":
                    offered = data.get("WORKERS_UUID", [])
                    logging.info(f"[AVAILABLE] {donor_ip} ofereceu {len(offered)}: {offered}")
                    if offered: got = True
        except Exception as e:
            logging.debug(f"[WORKER_REQUEST] {donor_ip} falhou: {e}")
    return got

def initiate_worker_release(owner_server_uuid, workers_list):
    owner_ip = owner_server_uuid  # aqui SERVER_UUID == IP
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5); s.connect((owner_ip, PORT))
            msg = {"SERVER_UUID": SERVER_UUID, "TASK": "COMMAND_RELEASE", "WORKERS_UUID": workers_list}
            send_json(s, msg); logging.info(f"[PAYLOAD→{owner_ip}] {msg}")
            ack = recv_json(s, 5)
            if ack and ack.get("RESPONSE") == "RELEASE_ACK":
                logging.info("[PROTOCOL] RELEASE_ACK recebido. Enviando RETURN aos workers...")
                for w in workers_list:
                    try:
                        with lock: wip = workers_controlled.get(w)
                        if not wip: continue
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ws:
                            ws.settimeout(5); ws.connect((wip, WORKER_PORT))
                            ret = {"TASK": "RETURN", "SERVER_RETURN": {"ip": owner_ip, "port": WORKER_PORT}}
                            send_json(ws, ret); logging.info(f"[PAYLOAD→worker {w}] {ret}")
                    except Exception as e:
                        logging.error(f"[RETURN] {w}: {e}")
                with lock:
                    for w in workers_list:
                        borrowed_workers.pop(w, None); workers_controlled.pop(w, None)
    except Exception as e:
        logging.error(f"[COMMAND_RELEASE] erro: {e}")

def send_release_completed(borrower_ip, workers_list):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5); s.connect((borrower_ip, PORT))
            msg = {"SERVER_UUID": SERVER_UUID, "RESPONSE": "RELEASE_COMPLETED", "WORKERS_UUID": workers_list}
            send_json(s, msg); logging.info(f"[PAYLOAD→{borrower_ip}] {msg}")
    except Exception as e:
        logging.error(f"[RELEASE_COMPLETED] erro: {e}")

def command_worker_redirect(workers_list, dest_ip, dest_port):
    time.sleep(0.5)
    for wid in workers_list:
        try:
            with lock:
                if wid not in workers_controlled: continue
                wip = workers_controlled[wid]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5); s.connect((wip, WORKER_PORT))
                cmd = {"TASK":"REDIRECT","SERVER_REDIRECT":{"ip":dest_ip,"port":dest_port}}
                send_json(s, cmd); logging.info(f"[PAYLOAD→worker {wid}] {cmd}")
        except Exception as e:
            logging.error(f"[REDIRECT] {wid}: {e}")

# ===================== HANDLER MASTER =====================
def receive_master(c, addr):
    try:
        data = recv_json(c, 5)
        if not data: return
        task = data.get("TASK")

        if task == "HEARTBEAT":
            send_json(c, hb_ok()); logging.info(f"[PAYLOAD←{addr[0]}] {hb_ok()}")

        elif task == "WORKER_REQUEST":
            req = data.get("REQUESTOR_INFO", {}) or {}
            tip = req.get("ip"); tport = req.get("port", WORKER_PORT)
            needed = int(req.get("needed", 1))
            with lock:
                allw = list(workers_controlled.items())
                lendable = max(0, len(allw) - MIN_KEEP_LOCAL)
                k = max(0, min(needed, lendable))
                chosen = random.sample(allw, k) if k > 0 else []
            if chosen:
                uuids = [u for (u, _ip) in chosen]
                resp = {"SERVER_UUID": SERVER_UUID, "RESPONSE": "AVAILABLE", "WORKERS_UUID": uuids}
                send_json(c, resp); logging.info(f"[PAYLOAD←{addr[0]}] {resp}")

                def _redir():
                    for u, _ in chosen:
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ws:
                                ws.settimeout(5); ws.connect((workers_controlled[u], WORKER_PORT))
                                cmd = {"TASK":"REDIRECT","SERVER_REDIRECT":{"ip":tip,"port":tport}}
                                send_json(ws, cmd); logging.info(f"[PAYLOAD→worker {u}] {cmd}")
                                time.sleep(0.2)
                        except Exception as e:
                            logging.error(f"[REDIRECT] {u}: {e}")
                threading.Thread(target=_redir, daemon=True).start()
            else:
                resp = {"SERVER_UUID": SERVER_UUID, "RESPONSE": "UNAVAILABLE"}
                send_json(c, resp); logging.info(f"[PAYLOAD←{addr[0]}] {resp}")

        elif task == "COMMAND_RELEASE":
            borrower_ip = c.getpeername()[0]
            wlist = data.get("WORKERS_UUID", [])
            ack = {"RESPONSE":"RELEASE_ACK","SERVER_UUID":SERVER_UUID,"WORKERS_UUID":wlist}
            send_json(c, ack); logging.info(f"[PAYLOAD→{borrower_ip}] {ack}")
            with lock:
                for w in wlist:
                    pending_returns[w] = borrower_ip

        else:
            if data.get("RESPONSE") == "RELEASE_COMPLETED":
                logging.info(f"[RELEASE_COMPLETED] recebido de {addr[0]} p/ {data.get('WORKERS_UUID')}")
    finally:
        try: c.close()
        except: pass

# ===================== WORKERS =====================
def receive_alive_worker(conn, addr):
    try:
        data = recv_json(conn, 5)
        if not data:
            conn.close(); return

        wid = data.get("WORKER_UUID")
        owner_uuid = data.get("SERVER_UUID")  # se emprestado
        if not wid:
            logging.warning(f"[WORKER] sem UUID de {addr}")
            conn.close(); return

        with lock:
            workers_controlled[wid] = addr[0]
            if owner_uuid and owner_uuid != SERVER_UUID:
                borrowed_workers[wid] = {"owner_server_uuid": owner_uuid, "timestamp": time.time()}
                logging.info(f"[WORKER] emprestado {wid} de {owner_uuid} conectado de {addr[0]}")
            else:
                logging.info(f"[WORKER] próprio {wid} conectado de {addr[0]}")
                send_json(conn, {"TASK": "QUERY", "USER": "11111111111"})
                logging.info(f"[PAYLOAD→{wid}] {{'TASK':'QUERY','USER':'11111111111'}}")

        with lock:
            borrower_ip = pending_returns.pop(wid, None)
        if borrower_ip:
            send_release_completed(borrower_ip, [wid])

        threading.Thread(target=manage_worker_connection, args=(conn, addr, wid), daemon=True).start()

    except Exception as e:
        logging.error(f"[receive_alive_worker] {e}")
        try: conn.close()
        except: pass

def manage_worker_connection(conn, addr, wid):
    try:
        conn.settimeout(30)
        while True:
            need = False
            with lock:
                if task_queue:
                    task_queue.pop(0); need = True
            if need:
                try:
                    query = {"TASK":"QUERY","USER":"11111111111"}
                    send_json(conn, query); logging.info(f"[PAYLOAD→{wid}] {query}")
                except Exception as e:
                    logging.error(f"[QUERY] {wid}: {e}"); break
            try:
                data = recv_json(conn, 1)
                if data: logging.info(f"[PAYLOAD←{wid}] {data}")
            except Exception:
                pass
            time.sleep(0.2)
    finally:
        try: conn.close()
        except: pass
        with lock:
            workers_controlled.pop(wid, None)
            borrowed_workers.pop(wid, None)

# ===================== LISTENERS =====================
def listen_masters():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT)); s.listen()
        logging.info(f"[MASTER] escutando {HOST}:{PORT}")
        while True:
            c, addr = s.accept()
            threading.Thread(target=receive_master, args=(c, addr), daemon=True).start()
    except Exception as e:
        logging.error(f"[listen_masters] {e}")

def listen_workers():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, WORKER_PORT)); s.listen()
        logging.info(f"[WORKER] escutando {HOST}:{WORKER_PORT}")
        while True:
            c, addr = s.accept()
            threading.Thread(target=receive_alive_worker, args=(c, addr), daemon=True).start()
    except Exception as e:
        logging.error(f"[listen_workers] {e}")

# ===================== MONITORES =====================
def monitor_request_workers():
    global last_request_time
    while True:
        time.sleep(1)
        with lock:
            qlen = len(task_queue)
            can = (time.time() - last_request_time) >= REQUEST_COOLDOWN
        if qlen >= QUEUE_THRESHOLD and can:
            if ask_for_workers():
                with lock: last_request_time = time.time()
                logging.info(f"[THRESHOLD] fila={qlen} ≥ {QUEUE_THRESHOLD} → pedido enviado.")

def monitor_release_workers():
    while True:
        time.sleep(2)
        with lock:
            q = len(task_queue)
            blist = list(borrowed_workers.keys())
            bcount = len(blist)
            owner_uuid = borrowed_workers[blist[0]]["owner_server_uuid"] if bcount else None
        if bcount > 0 and q < QUEUE_THRESHOLD and owner_uuid:
            logging.info(f"[SATURATION] normalizou (fila {q} < {QUEUE_THRESHOLD}). Devolvendo {bcount}...")
            initiate_worker_release(owner_uuid, blist)

def fake_load_generator():
    while True:
        if not ENABLE_FAKE_LOAD:
            time.sleep(1); continue
        with lock:
            now = time.time()
            for _ in range(FAKE_LOAD_RATE_PER_SEC):
                task_queue.append(now)
            task_queue[:] = [t for t in task_queue if now - t < 60]
            qlen = len(task_queue)
        logging.info(f"[FAKE_LOAD] fila={qlen}")
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
    while True: time.sleep(1)

if __name__ == "__main__":
    main()
