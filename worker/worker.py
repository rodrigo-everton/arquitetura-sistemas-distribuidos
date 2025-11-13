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

def conexao(s, worker_id, HOST, PORT, master_origin):
    """Tenta conectar e registrar o worker no master."""
    try:
        s.connect((HOST, PORT))
        # Payload de registro/re-registro. MASTER_ORIGIN é crucial para o master validar.
        worker_alive_msg = {
            "WORKER": "ALIVE",
            "WORKER_UUID": worker_id,
            "MASTER_ORIGIN": master_origin
        }
        msg = json.dumps(worker_alive_msg) + "\n"
        logging.info(f"[PAYLOAD] {worker_alive_msg}")
        s.sendall(msg.encode())
    except socket.error as e:
        logging.error(f"[Connection Error] Falha ao conectar em {HOST}:{PORT} - {e}")
        raise # Propaga o erro para o loop principal tratar a reconexão

if __name__ == '__main__':
    worker_id = str(uuid.uuid4())
    # O master inicial é a origem do worker
    master_origin_ip = '192.168.15.4'
    
    # Variáveis que controlam a conexão atual
    current_master_ip = master_origin_ip
    current_master_port = 5001
    
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                logging.info(f"Tentando conectar ao master {current_master_ip}:{current_master_port}")
                # Conecta e se registra com o master atual
                conexao(s, worker_id, current_master_ip, current_master_port, master_origin_ip)
                s.settimeout(45) # Timeout para receber dados
                
                buffer = b""
                while True: # Loop de comunicação com o master conectado
                    try:
                        # Leitura de dados de forma mais robusta
                        chunk = s.recv(4096)
                        if not chunk:
                            logging.warning("Conexão fechada pelo master. Reconectando...")
                            break
                        
                        buffer += chunk
                        # Processa mensagens completas (delimitadas por \n)
                        while b'\n' in buffer:
                            message_raw, buffer = buffer.split(b'\n', 1)
                            if not message_raw:
                                continue

                            answer = json.loads(message_raw.decode())
                            logging.info(f"[PAYLOAD] Recebido: {answer}")

                            task = answer.get("TASK")

                            # |5.3| Lógica para ordem de retorno ao master original
                            if task == "RETURN":
                                server_return = answer.get("SERVER_RETURN")
                                if server_return and "ip" in server_return and "port" in server_return:
                                    new_ip = server_return["ip"]
                                    new_port = server_return["port"]
                                    logging.info(f"[PROTOCOL] Recebida ordem de retorno para {new_ip}:{new_port}")
                                    
                                    # Atualiza o alvo da próxima conexão
                                    current_master_ip = new_ip
                                    current_master_port = new_port
                                    
                                    # Força a quebra do loop para reconectar
                                    raise ConnectionAbortedError("Retornando ao master original")
                                else:
                                    logging.error("[PROTOCOL] Payload de retorno inválido.")
                            
                            elif task == "QUERY":
                                output = {
                                    "WORKER_UUID": worker_id,
                                    "CPF": "11111111111",
                                    "SALDO": "25.000",
                                    "TASK": "QUERY",
                                    "STATUS": "OK"
                                }
                                time.sleep(0.2) # Simula trabalho
                                s.sendall((json.dumps(output) + "\n").encode())
                                logging.info(f"[PAYLOAD] Enviado: {output}")
                            
                            else:
                                logging.warning(f"Tarefa desconhecida recebida: {task}")

                    except (socket.timeout, ConnectionResetError):
                        logging.warning("Timeout ou conexão resetada. Reconectando...")
                        break
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logging.error(f"[Recv/JSON Error] {e}")
                        continue # Tenta ler o próximo fragmento do buffer
                    except ConnectionAbortedError: # Usado para o fluxo de retorno
                        break # Sai do loop interno para reconectar ao novo master
                    except Exception as e:
                        logging.error(f"Erro inesperado no loop de comunicação: {e}")
                        break
        
        except (socket.error, Exception) as e:
            logging.error(f"Erro no ciclo de conexão: {e}. Tentando novamente em 5 segundos...")
        
        # Pausa antes de tentar uma nova conexão
        time.sleep(5)
