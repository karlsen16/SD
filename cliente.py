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
from rich.console import Console
from rich.table import Table

EXCHANGE_NAME = "leilao_control"
leiloes_ativos = {}                 # dict {id_leilao: leilao com o campo "lances" adicionado}
data_lock = threading.Lock()        # mutex para acessar o dict acima, pois é compartilhado entre as 3 threads
registration_queue = queue.Queue()  # fila para registrar pedidos de escuta em leilões específicos
stop_event = threading.Event()      # evento para sinalizar fim de execução para threads em paralelo
console = Console()                 # para prints da interface


# ----- Assinaturas -----
def gerar_chaves(cliente_id):
    key = RSA.generate(2048)
    private_key = key

    pasta_cliente = os.path.join("Clientes", cliente_id)
    os.makedirs(pasta_cliente, exist_ok=True)
    pub_path = os.path.join(pasta_cliente, "public_key.der")

    public_key = key.publickey()
    with open(pub_path, "wb") as f:
        f.write(public_key.export_key(format="DER"))

    return private_key


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


# ----- Controle dos Leiloes Iniciados -----
def callback_leiloes(ch, method, properties, body):
    leilao = json.loads(body.decode("utf-8"))
    leilao["lances"] = []

    with data_lock:
        leiloes_ativos[leilao["id_leilao"]] = leilao
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ----- Controle das Notificações -----
def callback_notificacoes(ch, method, properties, body):
    lance = json.loads(body.decode("utf-8"))

    if lance.get("venceu"):
        with data_lock:
            leiloes_ativos.pop(lance['item'])
        print(f"\nCliente {lance['id']} venceu o leilão {lance['item']} com lance R$ {lance['valor']}")
    else:
        with data_lock:
            leiloes_ativos[lance['item']]['lances'].append(lance)
        print(f"\nNovo lance: Cliente {lance['id']} deu um lance de R$ {lance['valor']} no leilão {lance['item']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ----- Thread para escutar Leiloes Iniciados -----
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
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ----- Thread para escutar Leiloes de Interesse -----
def notification_worker(connection_params):
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

    result = ch.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    ch.basic_consume(queue=queue_name, on_message_callback=callback_notificacoes)

    try:
        # Enquanto não receber sinal para finalizar execução...
        while not stop_event.is_set():
            # Faz um bind para todos os pedidos na fila registration_queue
            while True:
                try:
                    leilao_id = registration_queue.get_nowait()
                except queue.Empty:
                    break
                ch.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=f"leilao_{leilao_id}")
                registration_queue.task_done()

            # Processa os lances enviados nos leilões de interesse por 1s
            conn.process_data_events(time_limit=1)
            time.sleep(0.1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ----- Interface -----
def maior_lance(leilao_id):
    with data_lock:
        leilao = leiloes_ativos.get(leilao_id)

    lances = leilao.get("lances", [])
    if not lances:
        return None

    maior = max(lances, key=lambda x: int(x["valor"]))
    return str(maior["valor"])


def mostrar_leiloes():
    table = Table(title="Leilões")

    table.add_column("ID", justify="center", style="cyan", no_wrap=True)
    table.add_column("Inicio", justify="center", style="green")
    table.add_column("Fim", style="magenta")
    table.add_column("Status", justify="right", style="yellow")
    table.add_column("Lances", justify="right", style="white")

    with data_lock:
        ativos = leiloes_ativos.values()
    for leilao in ativos:
        lance_valor = maior_lance(leilao['id_leilao'])
        if not lance_valor:
            lance_valor = "0"
        table.add_row(leilao['id_leilao'], leilao['inicio'][-8:], leilao['fim'][11:19], leilao['status'], "R$ " + lance_valor)

    console.print(table)


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
                mostrar_leiloes()

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
                    "item": leilao_id
                }

                mensagem = assinar_lance(lance, private_key)
                ch_pub.basic_publish(exchange=EXCHANGE_NAME,
                                         routing_key="lance_realizado",
                                         body=mensagem.encode("utf-8"))
                print(f"[UI] Lance enviado: R$ {valor} no leilão {leilao_id}")

                with data_lock:
                    leilao = leiloes_ativos.get(leilao_id)
                if leilao:
                    leilao['lances'].append({"id": cliente_id, "valor": valor, "item": leilao_id})

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


# ----- Main -----
def main():
    cliente_id = input("Digite seu ID: ")
    private_key = gerar_chaves(cliente_id)

    connection_params = pika.ConnectionParameters("localhost")

    #Thread para escutar a fila "leilao_iniciado" paralelamente
    t1 = threading.Thread(target=leilao_worker, args=(connection_params,), daemon=True)
    t1.start()

    #Thread para escutar dinâmicamente as filas dos leiloes de interesse
    #do cliente
    t2 = threading.Thread(target=notification_worker, args=(connection_params,), daemon=True)
    t2.start()

    #Interface para exibição dos leilões e realização dos lances
    interface_usuario(cliente_id, private_key, connection_params)

    print("Aguardando término das threads...")
    t1.join(timeout=2)
    t2.join(timeout=2)
    print("Encerrado.")


if __name__ == "__main__":
    main()
