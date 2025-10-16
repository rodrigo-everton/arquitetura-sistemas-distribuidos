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
    while True:
        HOST = '10.62.217.212'
        PORT = 5001

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            conection_thread = threading.Thread(target=conexao, args=(s, worker_id, HOST, PORT,))
            s.settimeout(30)
            conection_thread.start()
            conection_thread.join(timeout=30)
            try:
                data = s.recv(1024).decode()
                answer = json.loads(data)
                #print(data)
                user = answer.get("USER")
                #search = buscar_saldo_por_cpf(cpf)
                if data:
                    output = {
                            "WORKER_UUID":worker_id,
                            "CPF":"11111111111",
                            "SALDO":"25.000",
                            "TASK":"QUERY",
                            "STATUS":"OK"
                        }
                    time.sleep(1)
                    s.sendall(json.dumps(output).encode())
                    print(data)
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
                    s.sendall(json.dumps(output).encode())
                    print(data)
                    print(output)
                
            except json.JSONDecodeError:
                print("Erro de decode")