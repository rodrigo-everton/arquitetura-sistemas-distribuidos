SERVIDOR - WORKER
| 1 | Worker → Servidor | `{"WORKER": "ALIVE"}` | Apresentar-se e pedir tarefa. |
| 2 | Servidor → Worker | `{"TASK": "QUERY", "USER": "..."}` | Enviar uma tarefa de consulta. |
| 3 | Worker → Servidor | `{"STATUS": "OK", "SALDO": 99.99, ...}` | Devolver o resultado com sucesso. |
| 4 | Worker → Servidor | `{"STATUS": "NOK", ...}` | Informar que a execução da tarefa falhou. |


SERVIDOR - SERVIDOR
| 1 | Servidor A → Servidor B | `{"SERVER": "ALIVE"}` | Enviar um sinal de vida (heartbeat). |
| 1.1 | Servidor B → Servidor A | `{"SERVER": "ALIVE"}` | Enviar um sinal de vida (heartbeat). |

| 2 | Servidor A → Servidor B | `{"TASK": "WORKER_REQUEST", "WORKERS_NEEDED": 5}` | Enviar um pedido de trabalhadores emprestado. |
| 3 | Servidor A → Servidor B | `{"TASK": "WORKER_RESPONSE", "STATUS": "ACK | NACK"}` | Enviar uma resposta de pedido de trabalhadores emprestado (ACK: Servidor não saturado, NACK: Servidor saturado). |
| 3.1 | Servidor B → Servidor A | `{"TASK": "WORKER_RESPONSE", "STATUS": "ACK",  "WORKERS": ["WORKER_IP": ...] }` | Enviar uma resposta positiva de pedido de trabalhadores emprestado. |
| 3.2 | Servidor B → Servidor A | `{"TASK": "WORKER_RESPONSE", "STATUS": "NACK",  "WORKERS": [] }` | Enviar uma resposta negativa de pedido de trabalhadores emprestado. |