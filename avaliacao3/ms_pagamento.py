import json
import threading
import time
import uuid
from flask import Flask, request, jsonify
from flask_cors import CORS
import pika
import logging
import requests

EXCHANGE_NAME = "leilao_control"
RABBITMQ_HOST = "localhost"
PORT = 5003
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)


# ----- RabbitMQ -----
def get_channel():
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")
    return conn, ch

def publicar(routing_key, payload):
    try:
        conn, ch = get_channel()
        ch.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key,
                         body=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        conn.close()
        logging.info(f"Publicado {routing_key} -> {payload}")
    except Exception as e:
        logging.exception(f"Erro publishing: {e}")


def rabbit_consumer():
    logging.info("Conectando RabbitMQ (MS Pagamento)...")
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    result = ch.queue_declare(queue='', exclusive=True)
    fila = result.method.queue
    ch.queue_bind(exchange=EXCHANGE_NAME, queue=fila, routing_key='leilao_vencedor')

    def callback(ch, method, properties, body):
        try:
            payload = json.loads(body.decode("utf-8"))
            logging.info(f"Leilao vencedor recebido: {payload}")
            transaction_id = str(uuid.uuid4())
            link = f"http://mock-payments.local/pay/{transaction_id}"
            message = {
                "transaction_id": transaction_id,
                "link": link,
                "leilao": payload.get("leilao"),
                "vencedor": payload.get("cliente"),
                "valor": payload.get("valor")
            }
            publicar("link_pagamento", message)
        except Exception as e:
            logging.exception(f"Erro processando leilao_vencedor: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=fila, on_message_callback=callback)
    try:
        ch.start_consuming()
    except Exception as e:
        logging.exception(f"Rabbit consumer ended: {e}")
    finally:
        try:
            conn.close()
        except:
            pass


# ----- Endpoints -----
@app.route("/webhook", methods=["POST"])
def receive_webhook():
    data = request.get_json()
    if not data or "transaction_id" not in data or "status" not in data:
        return jsonify({"error":"transaction_id and status required"}), 400
    message = {
        "transaction_id": data["transaction_id"],
        "status": data["status"],
        "info": data.get("info", None)
    }
    publicar("status_pagamento", message)
    return jsonify({"ok": True})


# ----- Endpoints -----
if __name__ == "__main__":
    t = threading.Thread(target=rabbit_consumer, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
