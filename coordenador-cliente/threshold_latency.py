#THRESHOLD UTILIZANDO LATENCIA DAS RESPOSTAS DOS WORKERS

"""
THRESHOLD = 1.5
REQUEST_COOLDOWN = 10

    t0 = time.time()
    send_json(c, QUERY_WORKER)
    raw = c.recv(4096)
    t1 = time.time()

latency = (t1 - t0)
      now = time.time()
      with workers_lock:
        global last_request_time
        if latency >= THRESHOLD and (now - last_request_time >= REQUEST_COOLDOWN):
          ask_for_workers()
          last_request_time = now 

"""