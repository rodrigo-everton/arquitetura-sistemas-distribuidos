import socket
import json
import threading
import uuid
import time
        
def conexao(s, worker_id, HOST, PORT):
    try:
        s.connect((HOST,PORT))
        workerAlive = {"WORKER":"ALIVE",
                    "WORKER_UUID":worker_id,}
        msg = json.dumps(workerAlive) + "\n"
        s.sendall(msg.encode())
    except socket.error as e:
        print(f"[Connection error] {e}")

if __name__ == '__main__':
    worker_id = str(uuid.uuid4())
    current_master = '192.168.15.4'
    while True:
        HOST = current_master
        PORT = 5001
        s = None
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                conexao(s, worker_id, HOST, PORT)
                s.settimeout(30)
                redirect = False
                try:
                    while True:
                        try:
                            data = s.recv(1024).decode()
                        except (socket.error, UnicodeDecodeError) as e:
                            print(f"[Recv error] {e}")
                            break
                        if not data:
                            break
                        try:
                            answer = json.loads(data)
                        except json.JSONDecodeError as e:
                            print(f"[JSON error] {e}")
                            break
                        #print(answer)
                        if answer.get("TASK") == "REDIRECT":
                            try:
                                target = answer.get("MASTER_REDIRECT")
                                if target:
                                    current_master = target
                                    redirect = True
                                    s.close()
                                    break
                            except Exception as e:
                                print(f"[Redirect error] {e}")
                        elif answer.get("TASK") == "QUERY":
                            try:
                                output = {
                                        "WORKER_UUID":worker_id,
                                        "CPF":"11111111111",
                                        "SALDO":"25.000",
                                        "TASK":"QUERY",
                                        "STATUS":"OK"
                                        }
                                time.sleep(1)
                                s.sendall((json.dumps(output) + "\n").encode())
                                print(answer)
                                print(output)
                            except Exception as e:
                                print(f"[Query error] {e}")
                        else:
                            try:
                                output = {
                                        "WORKER":"EDUARDO",
                                        "CPF":"11111111111",
                                        "SALDO":0,
                                        "TASK":"QUERY",
                                        "STATUS":"NOK",
                                        "ERROR":"User not found"
                                        }
                                time.sleep(1)
                                s.sendall((json.dumps(output) + "\n").encode())
                                print(answer)
                                print(output)
                            except Exception as e:
                                print(f"[send error on else statement] {e}")
                except socket.timeout:
                    print("Timeout, reconnecting...")
                    time.sleep(0.5)
                    pass
                except Exception:
                    print("Error, reconnecting...")
                    time.sleep(0.5)
                    pass
        except Exception as e:
            print(f"Erro: {e}")
        