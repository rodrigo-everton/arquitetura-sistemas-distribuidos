import socket
import threading
import json
import time
import random
import statistics
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

HOST = "192.168.15.5"
PORT = 5000

MASTERS = {
    "servers": [
        #{"ip": "10.62.217.199", "name": "joao"},
        #{"ip": "10.62.217.16", "name": "thales.martins"},
        #{"ip": "10.62.217.209", "name": "thiago.machado"},
        #{"ip": "10.62.217.203", "name": "thiago.filho"},
        {"ip": "10.62.217.10", "name": "cassia"}

    ]
}

QUERY_WORKER = {
    "TASK": "QUERY",
    "USER": "11111111111"
}

SEND_ALIVE_MASTER = {"TASK": "HEARTBEAT"}
RESPOND_ALIVE_MASTER = {"TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
ASK_FOR_WORKERS = {"TASK": "WORKER_REQUEST"}
ASK_FOR_WORKERS_RESPONSE_NEGATIVE = {"RESPONSE": "UNAVAILABLE"}

THRESHOLD = 0.0005
REQUEST_COOLDOWN = 10

lock = threading.Lock()
masters_alive = {}
workers_controlled = {}
borrowed_workers = {}  # {uuid: {"master": name, "ip": ip, "timestamp": time}}
task_queue = []
latency_times = []
release_cmd = {}
expected_workers = {}
# last_request_time = 0 

def send_json(conn, obj):
    try:
        data = json.dumps(obj).encode("utf-8") + b"\n"
        conn.sendall(data)
    except Exception as e:
        logging.error(f"[ERROR] send json error: {e}")

def recv_json(conn):
    try:
        raw = conn.recv(4096)
        if not raw:
            return None
        try:
            return json.loads(raw.decode())
        except Exception:
            return None
    except Exception as e:
        logging.error(f"[ERROR] recv json error: {e}")
        return None

def send_alive_master():
    while True:
        for server in MASTERS["servers"]:
            ip = server["ip"]
            name = server["name"]
            if ip == HOST:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect((ip, PORT))
                    send_json(s, SEND_ALIVE_MASTER)
                    logging.info(f"[PAYLOAD] {SEND_ALIVE_MASTER}")
                    data = recv_json(s)
                    if data and data.get("RESPONSE") == "ALIVE":
                        with lock:
                            masters_alive[name] = ip
                        logging.info(f"[+] Master '{name}' ({ip}) está vivo.")
            except socket.timeout:
                with lock:
                    if name in masters_alive:
                        del masters_alive[name]
                logging.warning(f"[-] Timeout ao contatar '{name}' ({ip}).")
            except socket.error as e:
                with lock:
                    if name in masters_alive:
                        del masters_alive[name]
                logging.error(f"[-] Falha ao contatar '{name}' ({ip}): {e}")
            except Exception as e:
                logging.error(f"[-] Erro inesperado em send_alive_master: {e}")
            time.sleep(1)
        time.sleep(5)

def receive_master(c, addr):
    try:
        data = recv_json(c)
        if not data:
            c.close()
            return
        try:
            task = data.get("TASK")
        except (TypeError, AttributeError) as e:
            logging.error(f"[-] falha ao obter TASK de {addr}: {e}")
            c.close()
            return
        if task == "HEARTBEAT":
            try:
                send_json(c, RESPOND_ALIVE_MASTER)
                logging.info(f"[PAYLOAD] {RESPOND_ALIVE_MASTER}")
            except Exception as e:
                logging.error(f"[-] heartbeat response error {addr}: {e}")
        elif task == "WORKER_REQUEST":
            try:
                with lock:
                    if workers_controlled:
                        uuid, ip = random.choice(list(workers_controlled.items()))
                        response = {
                            "RESPONSE": "AVAILABLE",
                            "WORKERS": [{"WORKER_UUID": uuid, "WORKER_IP": ip}],
                        }
                    else:
                        response = ASK_FOR_WORKERS_RESPONSE_NEGATIVE
                try:
                    send_json(c, response)
                    logging.info(f"[PAYLOAD] {response}")
                except Exception as e:
                    logging.error(f"[-] worker request send error {addr}: {e}")
            except Exception as e:
                logging.error(f"[-] worker request response error {addr}: {e}")
        elif task == "COMMAND_RELEASE":
            try:
                master_name = data.get("MASTER_NAME", "unknown")
                master_ip = data.get("MASTER_IP", "unknown")
                workers_to_return = data.get("WORKERS", [])
                logging.info(f"\n[PROTOCOL] Recebido COMMAND_RELEASE do Master {master_name}")
                #print(f"[PROTOCOL] Workers a devolver: {len(workers_to_return)}")
                
                response = {"RESPONSE": "RELEASE_ACK", 
                            "MASTER": HOST,
                            "WORKERS": workers_to_return}
                send_json(c, response)
                logging.info(f"[PAYLOAD] {response}")
                #print(f"[PROTOCOL] Enviado RELEASE_ACK para {master_name}\n")
                
                threading.Thread(
                    target=process_worker_return,
                    args=(workers_to_return, master_name, master_ip),
                    daemon=True
                ).start()
            except Exception as e:
                logging.error(f"[-] Erro ao processar COMMAND_RELEASE: {e}")
    except Exception as e:
        logging.error(f"[-] unexpected receive_master error {addr}: {e}")
    finally:
        c.close()
def command_worker_redirect(workers_list, owner_master_ip):
    """Envia comando REDIRECT aos workers para retornarem ao master original"""
    time.sleep(1)
    
    for worker_uuid in workers_list:
        try:
            with lock:
                if worker_uuid in workers_controlled:
                    worker_ip = workers_controlled[worker_uuid]
                else:
                    continue
            
            # Conecta ao worker na porta de comando (5001)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((worker_ip, 5001))
                
                redirect_cmd = {
                    "MASTER:": HOST,
                    "TASK": "REDIRECT",
                    "MASTER_REDIRECT": owner_master_ip
                }
                send_json(s, redirect_cmd)
                logging.info(f"[PAYLOAD] {redirect_cmd}")
                logging.info(f"[PROTOCOL] Enviado REDIRECT ao worker {worker_uuid}")
                logging.info(f"[PROTOCOL] Novo master: {owner_master_ip}")

        except socket.timeout:
            logging.warning(f"[-] Timeout ao enviar REDIRECT para {worker_uuid}")
        except Exception as e:
            logging.error(f"[-] Erro ao enviar REDIRECT para {worker_uuid}: {e}")

def listen_masters():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()  #timeout aqui??
        logging.info(f"[MASTER] Escutando masters em {HOST}:{PORT}")
        while True:
            try:
                c, addr = s.accept()
                threading.Thread(target=receive_master, args=(c, addr), daemon=True).start()

            except socket.error as e:
                logging.error(f"[ERROR] master accept socket error: {e}")
            except Exception as e:
                logging.error(f"[ERROR] unexpected thread start error: {e}")
    except socket.error as e:
        logging.error(f"[ERROR] master listening socket error: {e}")
    except Exception as e:
        logging.error(f"[ERROR] unexpected listen_masters setup error: {e}")

def ask_for_workers():
    with lock:
        alive_list = list(masters_alive.items())
    
    for name, ip in alive_list:
        try:
            logging.info(f"[ASK] Pedindo workers para {name} ({ip})...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((ip, PORT))
                send_json(s, ASK_FOR_WORKERS)
                logging.info(f"[PAYLOAD] {ASK_FOR_WORKERS}")
                
                data = recv_json(s)
                if not data:
                    continue
                
                if data.get("RESPONSE") == "AVAILABLE":
                    worker_info = data["WORKERS"][0]
                    uuid = worker_info["WORKER_UUID"]
                    wip = worker_info["WORKER_IP"]
                    
                    with lock:
                        # MARCA COMO ESPERADO ANTES DO WORKER CONECTAR
                        expected_workers[uuid] = {
                            "master": name,
                            "ip": ip
                        }
                    
                    logging.info(
                        f"[OK] Worker {uuid} será emprestado por {name} "
                        f"(aguardando conexão de {wip})"
                    )
                    return True
                    
        except socket.timeout:
            logging.warning(f"[TIMEOUT] Timeout ao pedir worker de {name}")
        except socket.error as e:
            logging.error(f"[ERROR] Falha ao pedir worker de {name}: {e}")
        except Exception as e:
            logging.error(f"[ERROR] Erro inesperado com {name}: {e}")
    
    return False

def process_worker_return(workers_list, owner_master_name, owner_master_ip):
    """Remove workers emprestados após protocolo de devolução"""
    time.sleep(1)
    
    command_worker_redirect(workers_list, owner_master_ip)
    
    with lock:
        for worker_uuid in workers_list:
            if worker_uuid in borrowed_workers:
                del borrowed_workers[worker_uuid]
            if worker_uuid in workers_controlled:
                del workers_controlled[worker_uuid]
                logging.info(f"[PROTOCOL] Worker {worker_uuid} devolvido para {owner_master_name}")

def manage_worker_connection(conn, addr, uuid):
    global latency
    try:
        conn.settimeout(30)
        
        while True:
            try:
                data = recv_json(conn)
                if not data:
                    logging.warning(f"[!] Worker {uuid} desconectou")
                    break
                try:
                    with lock:
                        task_queue.append({"uuid": uuid, "timestamp": time.time()})
                    t0 = time.time()
                    send_json(conn, QUERY_WORKER)
                    logging.info(f"[PAYLOAD] {QUERY_WORKER}")
                    t1 = time.time()
                    latency = (t1 - t0)
                    with lock:
                        latency_times.append(latency)
                except Exception as e:
                    logging.error(f"[ERROR] Falha ao enviar QUERY para worker {uuid}: {e}")
                    break
            
            except socket.timeout:
                logging.warning(f"[TIMEOUT] Worker {uuid} não respondeu")
                break
            except Exception as e:
                logging.error(f"[ERROR] Worker {uuid}: {e}")
                break
    except Exception as e:
        logging.error(f"[ERROR] erro no setup da conexão com o worker {uuid}: {e}")
    finally:
        conn.close()
        with lock:
            if uuid in workers_controlled:
                del workers_controlled[uuid]
            if uuid in borrowed_workers:
                del borrowed_workers[uuid]

def receive_alive_worker(conn, addr):
    try:
        data = recv_json(conn)
        if not data:
            conn.close()
            return
        
        uuid = data.get("WORKER_UUID")
        master_origin = data.get("MASTER_ORIGIN")
        
        if not uuid:
            logging.error(f"[ERROR] Worker sem UUID de {addr}")
            conn.close()
            return
        
        # LÓGICA CORRETA DE VALIDAÇÃO
        with lock:
            # Caso 1: Worker próprio (conectando pela primeira vez ou reconectando)
            is_own_worker = (master_origin == HOST)
            
            # Caso 2: Worker emprestado esperado
            is_expected_borrowed = (uuid in expected_workers)
            
            # Caso 3: Worker já controlado (reconexão)
            is_reconnecting = (uuid in workers_controlled)
            
            # Aceita se for qualquer um dos casos acima
            if is_own_worker or is_expected_borrowed or is_reconnecting:
                workers_controlled[uuid] = addr[0]
                
                # Se era esperado, marca como emprestado
                if is_expected_borrowed:
                    expected_info = expected_workers[uuid]
                    borrowed_workers[uuid] = {
                        "master": expected_info["master"],
                        "ip": expected_info["ip"],
                        "timestamp": time.time()
                    }
                    del expected_workers[uuid]
                    logging.info(f"[WORKER] Worker emprestado {uuid} conectado de {addr[0]}")
                else:
                    logging.info(f"[WORKER] Worker próprio {uuid} registrado de {addr[0]}")
            else:
                # Rejeita workers não autorizados
                logging.warning(
                    f"[REJECT] Worker {uuid} rejeitado "
                    f"(origin: {master_origin}, não esperado)"
                )
                conn.close()
                return
        
        # Envia primeira query
        send_json(conn, QUERY_WORKER)
        logging.info(f"[PAYLOAD] {QUERY_WORKER}")
        
        # Inicia gerenciamento do worker
        threading.Thread(
            target=manage_worker_connection,
            args=(conn, addr, uuid),
            daemon=True
        ).start()
        
    except Exception as e:
        logging.error(f"[ERROR] Falha ao registrar worker de {addr}: {e}")
        try:
            conn.close()
        except:
            pass

def listen_workers():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT + 1))
        s.listen()
        logging.info(f"[WORKER] Escutando workers em {HOST}:{PORT+1}")
        while True:
            try:
                c, addr = s.accept()
                threading.Thread(target=receive_alive_worker, args=(c, addr), daemon=True).start()
            except socket.error as e:
                logging.error(f"[ERROR] erro de socket accept na escuta de workers: {e}")
            except Exception as e:
                logging.error(f"[ERROR] erro inesperado no start da thread de tasks para workers: {e}")
    except socket.error as e:
        logging.error(f"[ERROR] erro de socket na escuta de workers: {e}")
    except Exception as e:
        logging.error(f"[ERROR] erro inesperado na configuração da escuta de workers: {e}")

def monitor_saturation():
    """Monitora saturação de tasks e inicia protocolo de devolução se normalizar"""

    while True:
        time.sleep(5)
        
        with lock:
            current_load = len(task_queue)
            borrowed_count = len(borrowed_workers)
            borrowed_list = list(borrowed_workers.keys()) if borrowed_workers else []
            owner_master = None
            if borrowed_workers:
                owner_master = list(borrowed_workers.values())[0].get("master")

        logging.info(f"[MONITOR] Carga: {current_load} tasks | Workers emprestados: {borrowed_count}")

        if len(latency_times) > 1 and statistics.mean(latency_times) < THRESHOLD and borrowed_count > 0 and owner_master:
            logging.info(f"\n[SATURATION] Carga normalizada, Iniciando devolução de {borrowed_count} workers...")
            initiate_worker_release(owner_master, borrowed_list)
            print()   #faltando log de confirmação de devolução
                
        with lock:
            current_time = time.time()
            task_queue[:] = [t for t in task_queue if current_time - t["timestamp"] < 60]


def initiate_worker_release(master_name, workers_list):
    """Inicia protocolo COMMAND_RELEASE com master dono"""
    global release_cmd
    try:
        with lock:
            master_ip = None
            for name, ip in masters_alive.items():
                if name == master_name:
                    master_ip = ip
                    break
        
        if not master_ip:
            logging.warning(f"[-] Master {master_name} não está vivo para receber devolução")
            return
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((master_ip, PORT))

            global release_cmd
            release_cmd = {
                "TASK": "COMMAND_RELEASE",
                "MASTER": HOST,
                "WORKERS": workers_list
            }
            send_json(s, release_cmd)
            logging.info(f"[PAYLOAD] {release_cmd}")
            logging.info(f"[PROTOCOL] Enviado COMMAND_RELEASE para Master {master_name}")
            logging.info(f"[PROTOCOL] Workers: {workers_list}")

            ack = recv_json(s)
            if ack and ack.get("RESPONSE") == "RELEASE_ACK":
                logging.info(f"[PROTOCOL] {master_name} confirmou RELEASE_ACK")
                threading.Thread(
                    target=process_worker_return,
                    args=(workers_list, master_name, master_ip),
                    daemon=True
                ).start()
    
    except socket.timeout:
        logging.warning(f"[-] Timeout ao enviar COMMAND_RELEASE para {master_name}")
    except Exception as e:
        logging.error(f"[-] Erro ao iniciar COMMAND_RELEASE: {e}")

def monitor_errors():
    while True:
        time.sleep(5)
        with lock:
            if len(latency_times) > 1:
                avg_latency = statistics.mean(latency_times)
        
        if len(latency_times) > 1 and avg_latency >= THRESHOLD:
            ask_for_workers()

def main():
    logging.info("[SYSTEM] Master iniciando...")
    threading.Thread(target=send_alive_master, daemon=True).start()
    threading.Thread(target=listen_masters, daemon=True).start()
    threading.Thread(target=listen_workers, daemon=True).start()
    threading.Thread(target=monitor_saturation, daemon=True).start()
    threading.Thread(target=monitor_errors, daemon=True).start()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
