import socket
import json
import threading
import uuid
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

def conexao(s, worker_id, HOST, PORT, master_origin=None):
    try:
        s.connect((HOST, PORT))
        workerAlive = {
            "WORKER": "ALIVE",
            "WORKER_UUID": worker_id,
            "MASTER": HOST,
            "MASTER_ORIGIN": master_origin
        }
        msg = json.dumps(workerAlive) + "\n"
        logging.info(f"[PAYLOAD] {workerAlive}")
        s.sendall(msg.encode())
    except socket.error as e:
        logging.error(f"[Connection error] {e}")

if __name__ == '__main__':
    worker_id = str(uuid.uuid4())
    current_master = '192.168.15.5'
    master_origin = current_master
    
    while True:
        HOST = current_master
        PORT = 5001
        s = None
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Passa master_origin na conexão
                conexao(s, worker_id, HOST, PORT, master_origin)
                s.settimeout(30)
                redirect = False
                try:
                    while True:
                        try:
                            data = s.recv(1024).decode()
                        except (socket.error, UnicodeDecodeError) as e:
                            logging.error(f"[Recv error] {e}")
                            break
                        if not data:
                            break
                        try:
                            answer = json.loads(data)
                            logging.info(f"[PAYLOAD] {answer}")
                        except json.JSONDecodeError as e:
                            logging.error(f"[JSON error] {e}")
                            break
                        #print(answer)
                        if answer.get("TASK") == "REDIRECT":
                            try:
                                target = answer.get("MASTER_REDIRECT")
                                if target:
                                    current_master = target
                                    logging.info(f"[REDIRECT] Redirecionando para: {current_master}")
                                    redirect = True
                                    s.close()
                                    break
                            except Exception as e:
                                logging.error(f"[Redirect error] {e}")
                        
                        elif answer.get("TASK") == "QUERY":
                            try:
                                output = {
                                    "WORKER_UUID": worker_id,
                                    "CPF": "11111111111",
                                    "SALDO": "25.000",
                                    "TASK": "QUERY",
                                    "STATUS": "OK"
                                }
                                time.sleep(1)
                                s.sendall((json.dumps(output) + "\n").encode())
                                logging.info(f"[PAYLOAD] {output}")
                            except Exception as e:
                                logging.error(f"[Query error] {e}")
                        
                        else:
                            try:
                                output = {
                                    "WORKER": "EDUARDO",
                                    "CPF": "11111111111",
                                    "SALDO": 0,
                                    "TASK": "QUERY",
                                    "STATUS": "NOK",
                                    "ERROR": "User not found"
                                }
                                time.sleep(1)
                                s.sendall((json.dumps(output) + "\n").encode())
                                logging.info(f"[PAYLOAD] {output}")
                            except Exception as e:
                                logging.error(f"[send error on else statement] {e}")
                
                except socket.timeout:
                    logging.warning("Timeout, reconnecting...")
                    time.sleep(0.5)
                    pass
                except Exception:
                    logging.error("Error, reconnecting...")
                    time.sleep(0.5)
                    pass
        except Exception as e:
            logging.error(f"Erro: {e}")
            time.sleep(2)  # Aguarda antes de reconectar
