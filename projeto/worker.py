import socket, json, uuid, time, logging

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

WORKER_UUID   = str(uuid.uuid4())
current_master = "127.0.0.1" 
master_origin  = None             # preenchido quando for emprestado
PORT = 5001

def send_json(conn, obj):
    try: conn.sendall((json.dumps(obj) + "\n").encode())
    except Exception as e: logging.error(f"[send_json] {e}")

def recv_json(conn, timeout=30):
    conn.settimeout(timeout)
    try:
        raw = conn.recv(4096)
        if not raw: return None
        try: return json.loads(raw.decode().strip())
        except Exception: return None
    except Exception: return None

def connect_and_register(host_ip):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10); s.connect((host_ip, PORT))
        alive = {"WORKER":"ALIVE","WORKER_UUID":WORKER_UUID}
        if master_origin and master_origin != host_ip:
            alive["SERVER_UUID"] = master_origin  # dono original
        send_json(s, alive); logging.info(f"[PAYLOAD→{host_ip}] {alive}")

        while True:
            msg = recv_json(s, 30)
            if not msg: break
            logging.info(f"[PAYLOAD←{host_ip}] {msg}")
            task = msg.get("TASK")

            if task == "REDIRECT":
                target = msg.get("SERVER_REDIRECT", {})
                tip = target.get("ip"); tport = target.get("port", PORT)
                if tip:
                    new_origin = master_origin or host_ip
                    logging.info(f"[REDIRECT] indo para {tip}:{tport}")
                    return ("REDIRECT", tip, tport, new_origin)

            elif task == "RETURN":
                target = msg.get("SERVER_RETURN", {})
                tip = target.get("ip"); tport = target.get("port", PORT)
                if tip:
                    logging.info(f"[RETURN] voltando ao dono {tip}:{tport}")
                    return ("RETURN", tip, tport, None)

            elif task == "QUERY":
                time.sleep(1)
                resp = {"WORKER_UUID":WORKER_UUID,"TASK":"QUERY","STATUS":"OK",
                        "CPF":"11111111111","SALDO":"25.000"}
                send_json(s, resp); logging.info(f"[PAYLOAD→{host_ip}] {resp}")

            else:
                time.sleep(1)
                resp = {"WORKER":"EDUARDO","TASK":"QUERY","STATUS":"NOK",
                        "ERROR":"User not found","CPF":"11111111111","SALDO":0}
                send_json(s, resp); logging.info(f"[PAYLOAD→{host_ip}] {resp}")
    return ("DISCONNECT", None, None, None)

if __name__ == "__main__":
    while True:
        action, tip, tport, origin = connect_and_register(current_master)
        if action in ("REDIRECT", "RETURN"):
            if action == "REDIRECT" and origin:
                master_origin = origin
            elif action == "RETURN":
                master_origin = None
            current_master = tip
            time.sleep(0.5); continue
        logging.warning("Desconectado. Reconnecting in 2s...")
        time.sleep(2)
