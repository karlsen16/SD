import json
import os
import threading
import base64
import queue
import time
import pika
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

EXCHANGE_NAME = "leilao_control"
leiloes_ativos = {}        # dict {id_leilao: descricao}
leiloes_de_interesse = {}  # dict {id_leilao: ultimo_lance}
data_lock = threading.Lock()
registration_queue = queue.Queue()
stop_event = threading.Event()


def carregar_ou_gerar_chaves(cliente_id):
    pasta_cliente = os.path.join("Clientes", cliente_id)
    os.makedirs(pasta_cliente, exist_ok=True)

    priv_path = os.path.join(pasta_cliente, "private_key.der")
    pub_path = os.path.join(pasta_cliente, "public_key.der")

    if os.path.exists(priv_path) and os.path.exists(pub_path):
        with open(priv_path, "rb") as f:
            private_key = RSA.import_key(f.read())
        with open(pub_path, "rb") as f:
            public_key = RSA.import_key(f.read())
    else:
        key = RSA.generate(2048)
        private_key = key
        public_key = key.publickey()
        with open(priv_path, "wb") as f:
            f.write(private_key.export_key(format="DER"))
        with open(pub_path, "wb") as f:
            f.write(public_key.export_key(format="DER"))

    return private_key, public_key


def assinar_lance(lance, private_key):
    try:
        lance_bytes = json.dumps(lance, ensure_ascii=False).encode("utf-8")
        h = SHA256.new(lance_bytes)
        assinatura = pkcs1_15.new(private_key).sign(h)
        assinatura_b64 = base64.b64encode(assinatura).decode("utf-8")
        return json.dumps(lance, ensure_ascii=False) + "||" + assinatura_b64
    except Exception as e:
        print(f"\n [!] Erro ao assinar -> {e}")
        return json.dumps(lance, ensure_ascii=False) + "|| "


def callback_leiloes(ch, method, properties, body):
    try:
        leilao = json.loads(body.decode("utf-8"))
    except Exception as e:
        print(f"\n [callback_leiloes] JSON inválido: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    with data_lock:
        leiloes_ativos[leilao["id_leilao"]] = leilao["descricao"]
    print(f"\nNovo leilão ativo: {leilao['id_leilao']} - {leilao['descricao']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def callback_notificacoes(ch, method, properties, body):
    try:
        lance = json.loads(body.decode("utf-8"))
    except Exception as e:
        print(f"\n [callback_leiloes] JSON inválido: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if lance.get("venceu"):
        print(f"\nCliente {lance['id']} venceu o leilão {lance['item']} com lance R$ {lance['valor']}")
        with data_lock:
            leiloes_ativos.pop(lance['item'], None)
    else:
        print(f"\nNovo lance: Cliente {lance['id']} deu R$ {lance['valor']} no leilão {lance['item']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def leilao_worker(connection_params):
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    result = ch.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    ch.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key="leilao_iniciado")
    ch.basic_consume(queue=queue_name, on_message_callback=callback_leiloes)

    try:
        ch.start_consuming()
    except Exception as e:
        print("[leilao_worker] exceção:", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def notification_worker(connection_params):
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    result = ch.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    ch.basic_consume(queue=queue_name, on_message_callback=callback_notificacoes)

    print("[notification_worker] fila única criada:", queue_name)
    try:
        while not stop_event.is_set():
            processed = False
            while True:
                try:
                    leilao_id = registration_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    ch.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=f"leilao_{leilao_id}")
                    print(f"[notification_worker] registrado para leilao_{leilao_id}")
                except Exception as e:
                    print(f"[notification_worker] erro ao bind leilao_{leilao_id}: {e}")
                registration_queue.task_done()
                processed = True

            conn.process_data_events(time_limit=1)
            if processed:
                time.sleep(0.1)
    except Exception as e:
        print("[notification_worker] exceção:", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def interface_usuario(cliente_id, private_key, connection_params):
    conn_pub = pika.BlockingConnection(connection_params)
    ch_pub = conn_pub.channel()
    ch_pub.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    try:
        while True:
            print("\n=== MENU CLIENTE ===")
            print("1. Listar leilões ativos")
            print("2. Dar lance")
            print("3. Sair")
            escolha = input("> ").strip()

            if escolha == "1":
                with data_lock:
                    if not leiloes_ativos:
                        print("Nenhum leilão ativo.")
                    else:
                        print("Leilões ativos:")
                        for lid, desc in leiloes_ativos.items():
                            print(f"  {lid}: {desc}")

            elif escolha == "2":
                with data_lock:
                    if not leiloes_ativos:
                        print("Nenhum leilão disponível para lances.")
                        continue
                leilao_id = input("Digite o ID do leilão: ").strip()
                with data_lock:
                    if leilao_id not in leiloes_ativos:
                        print("Leilão inválido!")
                        continue

                valor = input("Digite o valor do lance: ").strip()
                lance = {
                    "id": cliente_id,
                    "valor": valor,
                    "item": leilao_id,
                    "venceu": False
                }

                mensagem = assinar_lance(lance, private_key)
                ch_pub.basic_publish(exchange=EXCHANGE_NAME,
                                         routing_key="lance_realizado",
                                         body=mensagem.encode("utf-8"))
                print(f"[UI] Lance enviado: R$ {valor} no leilão {leilao_id}")

                with data_lock:
                    prev = leiloes_de_interesse.get(leilao_id)
                    if prev is None or int(valor) > int(prev):
                        leiloes_de_interesse[leilao_id] = valor

                registration_queue.put(leilao_id)

            elif escolha == "3":
                print("Saindo...")
                break
            else:
                print("Opção inválida!")
    finally:
        try:
            conn_pub.close()
        except Exception:
            pass
        stop_event.set()


def main():
    cliente_id = input("Digite seu ID: ")
    private_key, _ = carregar_ou_gerar_chaves(cliente_id)

    connection_params = pika.ConnectionParameters("localhost")

    t1 = threading.Thread(target=leilao_worker, args=(connection_params,), daemon=True)
    t1.start()

    t2 = threading.Thread(target=notification_worker, args=(connection_params,), daemon=True)
    t2.start()

    interface_usuario(cliente_id, private_key, connection_params)

    print("Aguardando término das threads...")
    t1.join(timeout=2)
    t2.join(timeout=2)
    print("Encerrado.")


if __name__ == "__main__":
    main()
