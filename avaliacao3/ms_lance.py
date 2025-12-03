import threading
import time
import pika
import json
from flask import Flask, request, jsonify
import logging

EXCHANGE_NAME = "leilao_control"
RABBITMQ_HOST = "localhost"
PORT = 5002
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
leiloes_ativos = {}
ativos_lock = threading.Lock()


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
        logging.exception(f"[!] erro ao publicar: {e}")


# ----- Endpoints -----
@app.route("/lances", methods=["POST"])
def receber_lance():
    data = request.get_json()
    if not data or "cliente" not in data or "leilao" not in data or "valor" not in data:
        return jsonify({"error":"cliente, leilao e valor required"}), 400
    leilao = data["leilao"]
    com_valor = None
    with ativos_lock:
        if leilao not in leiloes_ativos:
            logging.info(f"Lance recebido para leilao inativo/inexistente: {leilao}")
            publicar("lance_invalidado", data)
            return jsonify({"ok": False, "reason": "leilao inativo/inexistente"}), 400
        atual = leiloes_ativos[leilao]
        if atual is None:
            com_valor = True
        else:
            try:
                if data["valor"] > atual["valor"]:
                    com_valor = True
                else:
                    com_valor = False
            except Exception:
                com_valor = False

        if com_valor:
            data["venceu"] = False
            leiloes_ativos[leilao] = data
            publicar("lance_validado", data)
            return jsonify({"ok": True}), 200
        else:
            publicar("lance_invalidado", data)
            return jsonify({"ok": False, "reason": "valor menor ou igual ao atual"}), 400


# ----- Main -----
def rabbit_consumer():
    logging.info("Conectando RabbitMQ (MS Lance)...")
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    result = ch.queue_declare(queue='', exclusive=True)
    fila = result.method.queue
    for rk in ['leilao_iniciado', 'leilao_finalizado']:
        ch.queue_bind(exchange=EXCHANGE_NAME, queue=fila, routing_key=rk)

    def callback(ch, method, properties, body):
        try:
            rk = method.routing_key
            payload = json.loads(body.decode("utf-8"))
            lid = str(payload.get("leilao"))
            logging.info(f"Evento {rk} recebido: {payload}")
            with ativos_lock:
                if rk == 'leilao_iniciado':
                    leiloes_ativos[lid] = None
                elif rk == 'leilao_finalizado':
                    vencedor = leiloes_ativos.get(lid)
                    if vencedor is not None:
                        vencedor['venceu'] = True
                        publicar('leilao_vencedor', vencedor)
                    if lid in leiloes_ativos:
                        del leiloes_ativos[lid]
        except Exception as e:
            logging.exception(f"Erro processando evento: {e}")
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

if __name__ == "__main__":
    threading.Thread(target=rabbit_consumer, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
