import pika
import json
import base64
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import os

EXCHANGE_NAME = "leilao_control"
leiloes_ativos = {}
melhores_lances = {}


def verifica_assinatura(lance, assinatura):
    try:
        data = json.dumps(lance, ensure_ascii=False).encode()
        pasta_cliente = os.path.join("Clientes", lance['id'])
        pub_path = os.path.join(pasta_cliente, "public_key.der")

        client_public_key = RSA.import_key(open(pub_path, "rb").read())
        h = SHA256.new(data)
        pkcs1_15.new(client_public_key).verify(h, assinatura)
        return True
    except (ValueError, TypeError) as e:
        print(f"\n [!] Erro ao verificar assinatura de lance (Cliente:{lance['id']}, Leilao:{lance['item']}): {e}")
        return False
    except FileNotFoundError:
        print(f"\n [!] Chave pública do cliente {lance['id']} não encontrada.")
        return False


def callback_leiloes(ch, method, properties, body):
    try:
        leilao = json.loads(body.decode("utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        leilao_id = leilao['id_leilao']

        if leilao['status'] == "ativo":
            leiloes_ativos[leilao_id] = leilao
            melhores_lances[leilao_id] = None
            print(f"Novo leilão ativo: {leilao_id}")

        elif leilao['status'] == "encerrado":
            del leiloes_ativos[leilao_id]

            # Se houve algum lance válido, publica vencedor
            vencedor = melhores_lances.get(leilao_id)
            if vencedor is not None:
                vencedor["venceu"] = True
                message = json.dumps(vencedor, ensure_ascii=False)
                ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='leilao_vencedor', body=message.encode("utf-8"))
            if leilao_id in melhores_lances:
                del melhores_lances[leilao_id]
                print(f"Leilão encerrado: {leilao_id}")
    except Exception as e:
        print(f"\n [!] Erro ao processar leilao: {e}")


def callback_lances(ch, method, properties, body):
    try:
        body_str = body.decode('utf-8')
        json_part, assinatura_b64 = body_str.split("||")
        lance = json.loads(json_part)
        assinatura = base64.b64decode(assinatura_b64)
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if (verifica_assinatura(lance, assinatura) and
             lance['item'] in leiloes_ativos and
             (melhores_lances[lance['item']] is None or
              int(lance['valor']) > int(melhores_lances[lance['item']]['valor']))):

            melhores_lances[lance['item']] = lance
            print(f"Lance válido recebido: Cliente {lance['id']} -> R$ {lance['valor']} no leilão {lance['item']}")
            message = json.dumps(lance, ensure_ascii=False)
            ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='lance_validado', body=message.encode("utf-8"))
    except Exception as e:
        print(f"[!] Erro ao processar lance: {e}")


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

for queue in ['lance_realizado', 'leilao_iniciado', 'leilao_finalizado']:
    channel.queue_declare(queue=queue, exclusive=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue, routing_key=queue)

    if queue == 'lance_realizado':
        cb = callback_lances
    else:
        cb = callback_leiloes

    channel.basic_consume(queue=queue, on_message_callback=cb)

print(' [*] Esperando novos leilões e lances.')
channel.start_consuming()
