import socket
import json
import threading
import uuid
import time
        
def conexao(s, worker_id, HOST, PORT):
    s.connect((HOST,PORT))
    workerAlive = {"WORKER":"ALIVE",
                   "WORKER_UUID":worker_id,}
    msg = json.dumps(workerAlive) + "\n"
    s.sendall(msg.encode())

if __name__ == '__main__':
    worker_id = str(uuid.uuid4())
    current_master = '10.62.217.17'
    while True:
        HOST = current_master
        PORT = 5001
        s = None
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                conection_thread = threading.Thread(target=conexao, args=(s, worker_id, HOST, PORT,))
                conection_thread.start()
                s.settimeout(30)
                conection_thread.join()
                redirect = False
                try:
                    while True:
                        data = s.recv(1024).decode()
                        if not data:
                            break
                        answer = json.loads(data)
                        #print(answer)               
                        if answer.get("TASK") == "REDIRECT":
                            target = answer.get("MASTER_REDIRECT")
                            if target:
                                current_master = target
                                redirect = True
                                s.close()
                                break
                        if answer.get("TASK") == "QUERY":
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
                        else:
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
                except Exception:
                    print("Error, reconnecting...")
                    time.sleep(0.5)
                    pass
        except Exception as e:
            print(f"Erro: {e}")
        