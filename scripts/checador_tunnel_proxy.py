#!/usr/bin/env python3
"""TCP proxy local para exponer el túnel Hikvision al contenedor Django.

El túnel QNAP publica el checador en loopback del host. Este proxy escucha en
las IPs gateway de Docker para que el contenedor `web` pueda llegar al checador
sin exponer el puerto públicamente.
"""

import os
import select
import socket
import threading

LISTEN_HOSTS = [
    host.strip()
    for host in os.getenv("CHECADOR_PROXY_LISTEN_HOSTS", "172.23.0.1,172.22.0.1").split(",")
    if host.strip()
]
LISTEN_PORT = int(os.getenv("CHECADOR_PROXY_LISTEN_PORT", "28073"))
TARGET_HOST = os.getenv("CHECADOR_PROXY_TARGET_HOST", "127.0.0.1")
TARGET_PORT = int(os.getenv("CHECADOR_PROXY_TARGET_PORT", "28073"))
BUFFER_SIZE = 65536
IDLE_TIMEOUT_SECONDS = 60


def relay(src, dst):
    try:
        while True:
            readable, _, _ = select.select([src, dst], [], [], IDLE_TIMEOUT_SECONDS)
            if not readable:
                return
            for sock in readable:
                try:
                    data = sock.recv(BUFFER_SIZE)
                except OSError:
                    return
                if not data:
                    return
                try:
                    (dst if sock is src else src).sendall(data)
                except OSError:
                    return
    finally:
        for sock in (src, dst):
            try:
                sock.close()
            except OSError:
                pass


def serve(host):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, LISTEN_PORT))
    srv.listen(50)
    print(f"checador proxy listening on {host}:{LISTEN_PORT} -> {TARGET_HOST}:{TARGET_PORT}", flush=True)
    while True:
        client, _ = srv.accept()
        try:
            target = socket.create_connection((TARGET_HOST, TARGET_PORT), timeout=10)
        except OSError:
            client.close()
            continue
        threading.Thread(target=relay, args=(client, target), daemon=True).start()


for listen_host in LISTEN_HOSTS:
    threading.Thread(target=serve, args=(listen_host,), daemon=True).start()

threading.Event().wait()
