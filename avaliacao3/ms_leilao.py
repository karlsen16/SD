import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import pika
import json
import time
import os

EXCHANGE_NAME = "leilao_control"
RABBITMQ_HOST = "localhost"
FORMATO_TIME = "%Y-%m-%d %H:%M:%S"
PORT = 5001
app = Flask(__name__)
leiloes = []
leiloes_lock = threading.Lock()


# ----- RabbitMQ -----
def get_channel():
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")
    return conn, ch


def publicar_leilao(leilao, routing_key):
    try:
        conn, ch = get_channel()
        msg = json.dumps(leilao, ensure_ascii=False).encode("utf-8")
        ch.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=msg)
        conn.close()
        print(f"[x] publicado {routing_key} -> {leilao['leilao']}")
    except Exception as e:
        print(f"[!] erro ao publicar: {e}")


# ----- Endpoints -----
@app.route("/leiloes", methods=["POST"])
def criar_leilao():
    data = request.get_json()
    if not data:
        return jsonify({"error":"JSON body required"}), 400
    required = ["leilao", "descricao", "inicio", "fim"]
    for k in required:
        if k not in data:
            return jsonify({"error": f"{k} required"}), 400
    with leiloes_lock:
        for l in leiloes:
            if l["leilao"] == data["leilao"]:
                return jsonify({"erro":"leilao já existe"}), 400
        leilao = {
            "leilao": str(data["leilao"]),
            "descricao": data["descricao"],
            "inicio": data["inicio"],
            "fim": data["fim"],
            "status": "programado"
        }
        leiloes.append(leilao)
    return jsonify({"ok": True, "leilao": leilao}), 201


@app.route("/leiloes", methods=["GET"])
def listar_leiloes():
    with leiloes_lock:
        return jsonify(leiloes)


# ----- Main -----
def scheduler_loop(poll_seconds=1):
    while True:
        now = datetime.now()
        remover = []
        with leiloes_lock:
            for leilao in leiloes:
                inicio = datetime.strptime(leilao["inicio"], FORMATO_TIME)
                fim = datetime.strptime(leilao["fim"], FORMATO_TIME)
                if leilao["status"] != "ativo" and now >= inicio:
                    leilao["status"] = "ativo"
                    publicar_leilao(leilao, "leilao_iniciado")
                if leilao["status"] == "ativo" and now >= fim:
                    publicar_leilao(leilao, "leilao_finalizado")
                    remover.append(leilao)
            for l in remover:
                leiloes.remove(l)
                print(f"[x] leilao removido da lista -> {l['leilao']}")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
