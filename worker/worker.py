import socket
import json
import psycopg2
import threading
import uuid
from psycopg2.extras import RealDictCursor

PG_HOST = "127.0.0.1"
PG_PORT = "5432"
PG_DB = "bank"
PG_USER = "postgres"
PG_PASS = "ceub123456"

def pg_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    
def buscar_saldo_por_cpf(cpf: str):
    with pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT u.cpf, u.nome, s.skdo
            FROM users u
            JOIN conta as s on (s.fkid_cliente = u.id)
            WHERE u.cpf = %s
        """, (cpf,))
        row = cur.fetchone()
        if not row:
            return None
        return (row["cpf"], row["nome"], float(row["skdo"]))
    
def conexao(s, HOST, PORT):
    s.connect((HOST,PORT))
    workerAlive = {"WORKER":"ALIVE"}
    msg = json.dumps(workerAlive) + "\n"
    s.sendall(msg.encode())

if __name__ == '__main__':
    while True:
        HOST = '10.62.217.32'
        PORT = 5900
        worker_id = str(uuid.uuid4())

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            conection_thread = threading.Thread(target=conexao, args=(s, HOST, PORT,))
            conection_thread.start()
            conection_thread.join(timeout=30)
            if conection_thread == None:
                print("Timeout")
            try:
                data = s.recv(1024).decode()
                answer = json.loads(data)
                #print(data)
                cpf = answer.get("USER")
                search = buscar_saldo_por_cpf(cpf)
                if search:
                    output = {
                            "WORKER":"EDUARDO",
                            "CPF":search[0],
                            "SALDO":search[2],
                            "TASK":"QUERY",
                            "STATUS":"OK"
                        }
                    s.sendall(json.dumps(output).encode())
                    print("✓")
                else:
                    output = {
                            "WORKER":"EDUARDO",
                            "CPF":cpf,
                            "SALDO":0,
                            "TASK":"QUERY",
                            "STATUS":"NOK",
                            "ERROR":"User not found"
                        }
                    s.sendall(json.dumps(output).encode())
                    print(search)
                
            except json.JSONDecodeError:
                print("Erro de decode")