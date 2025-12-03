import threading
import json
import time
import queue
import logging
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import requests
import pika

MS_LEILAO_URL = "http://localhost:5001"
MS_LANCE_URL  = "http://localhost:5002"
MS_PAG_URL    = "http://localhost:5003"
RABBITMQ_HOST = "localhost"
EXCHANGE_NAME = "leilao_control"
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
interesses = {}
interesses_lock = threading.Lock()
filas_sse = {}
filas_sse_lock = threading.Lock()


# ----- Utilitários -----
def enviar_evento_cliente(client, evento, data):
    with filas_sse_lock:
        fila = filas_sse.get(client)
    if fila:
        payload = {"event": evento, "data": data}
        try:
            fila.put_nowait(payload)
        except queue.Full:
            logging.warning(f"Fila do cliente {client} cheia")


def broadcast_interessados(leilao, evento, data):
    lista = []
    with interesses_lock:
        for cliente, leiloes in interesses.items():
            if leilao in leiloes:
                lista.append(cliente)
    if lista:
        logging.info(
            f"BROADCAST -> Enviando evento {evento} do leilão {leilao} para {len(lista)} clientes.")
    else:
        logging.warning(
            f"BROADCAST -> Nenhum cliente interessado para o leilão {leilao}. Clientes registrados: {interesses.keys()}")
    for cliente in lista:
        enviar_evento_cliente(cliente, evento, data)


# ----- Endpoints -----
@app.route("/leiloes", methods=["POST"])
def criar_leilao():
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "JSON body required"}), 400
    resp = requests.post(f"{MS_LEILAO_URL}/leiloes", json=payload)
    return resp.content, resp.status_code, resp.headers.items()


@app.route("/leiloes", methods=["GET"])
def listar_leiloes():
    resp = requests.get(f"{MS_LEILAO_URL}/leiloes")
    return resp.content, resp.status_code, resp.headers.items()


@app.route("/lances", methods=["POST"])
def efetuar_lance():
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "JSON body required"}), 400
    resp = requests.post(f"{MS_LANCE_URL}/lances", json=payload)
    return resp.content, resp.status_code, resp.headers.items()


@app.route("/interesse", methods=["POST"])
def registrar_interesse():
    data = request.get_json()
    if not data or "cliente" not in data or "leilao" not in data:
        return jsonify({"error": "cliente and leilao required"}), 400
    cliente = data["cliente"]
    leilao = data["leilao"]

    with interesses_lock:
        interesses.setdefault(cliente, set()).add(leilao)

    logging.info("Cliente %s registrado para leilao %s", cliente, leilao)
    return jsonify({"ok": True})


@app.route("/interesse", methods=["DELETE"])
def cancelar_interesse():
    data = request.get_json()
    if not data or "cliente" not in data or "leilao" not in data:
        return jsonify({"error": "cliente and leilao required"}), 400
    cliente = data["cliente"]
    leilao = data["leilao"]

    with interesses_lock:
        if cliente in interesses and leilao in interesses[cliente]:
            interesses[cliente].remove(leilao)
    logging.info(f"Cliente {cliente} não possui mais interesse no leilao {leilao}")
    return jsonify({"ok": True})


@app.route("/sse/<cliente>")
def sse_stream(cliente):
    fila = queue.Queue(maxsize=100)
    with filas_sse_lock:
        filas_sse[cliente] = fila
    logging.info(f"Cliente SSE conectado: {cliente} (Fila registrada)")

    def event_stream():
        try:
            yield f"event: connected\ndata: {json.dumps({'msg': 'connected', 'cliente': cliente})}\n\n"
            while True:
                try:
                    item = fila.get(timeout=15)
                    evento = item["event"]
                    data = item["data"]
                    yield f"event: {evento}\n"
                    yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    yield "\n"

        except GeneratorExit:
            logging.info(f"Cliente SSE desconectado (generator exit): {cliente}")
        except Exception as e:
            logging.error(f"Erro SSE fatal para {cliente}: {e}")

        finally:
            with filas_sse_lock:
                if filas_sse.get(cliente) is fila:
                    del filas_sse[cliente]
                    logging.info(f"Limpeza do cliente SSE finalizada e removida de filas_sse: {cliente}")
                else:
                    logging.warning(f"Fila do cliente {cliente} já foi substituída ou removida. Limpeza ignorada.")
    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")


# ----- MOM (RabbitMQ) -----
def comunicacao_interna():
    logging.info("Iniciando RabbitMQ...")
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    result = ch.queue_declare(queue='', exclusive=True)
    fila = result.method.queue

    routing_keys = ['lance_validado', 'lance_invalidado', 'leilao_vencedor', 'link_pagamento', 'status_pagamento']
    for rk in routing_keys:
        ch.queue_bind(exchange=EXCHANGE_NAME, queue=fila, routing_key=rk)

    def callback(ch, method, properties, body):
        try:
            evento = method.routing_key
            payload = json.loads(body.decode("utf-8"))
            logging.info(f"Evento RabbitMQ recebido: {evento} -> {payload}")

            if 'leilao' in payload:
                leilao = str(payload['leilao'])
                logging.info(f"MOM -> Leilão: {leilao}, Evento: {evento}. Tentando broadcast...")
                if evento == 'lance_validado':
                    broadcast_interessados(leilao, 'lance_validado', payload)
                elif evento == 'lance_invalidado':
                    broadcast_interessados(leilao, 'lance_invalido', payload)
                elif evento == 'leilao_vencedor':
                    broadcast_interessados(leilao, 'leilao_vencedor', payload)
                else:
                    broadcast_interessados(leilao, evento, payload)
            else:
                with filas_sse_lock:
                    for cid in list(filas_sse.keys()):
                        enviar_evento_cliente(cid, evento, payload)
        except Exception as e:
            logging.exception(f"Erro ao processar evento RabbitMQ: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=fila, on_message_callback=callback)

    try:
        ch.start_consuming()
    except Exception as e:
        logging.exception(f"RabbitMQ finalizado com erro: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        logging.info("RabbitMQ encerrado.")


# ----- Main -----
def iniciar_consumo():
    threading.Thread(target=comunicacao_interna, daemon=True).start()


if __name__ == "__main__":
    iniciar_consumo()
    app.run(host="0.0.0.0", port=5000, threaded=True)
