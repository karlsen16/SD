import pika
import json
import base64
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import os

EXCHANGE_NAME = "leilao_control"
leiloes_ativos = {}     # dict {id_leilao: melhor lance}


# ----- Verificação das Assinaturas -----
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


# ----- Controle dos Leiloes -----
def callback_leiloes(ch, method, properties, body):
    leilao = json.loads(body.decode("utf-8"))
    leilao_id = leilao['id_leilao']

    if leilao['status'] == "ativo":
        #Lances são atualizados na callback_lances
        leiloes_ativos[leilao_id] = None
        print(f" [v] Novo leilão ativo: {leilao_id}")

    elif leilao['status'] == "encerrado":
        vencedor = leiloes_ativos[leilao_id]
        if vencedor is not None:
            vencedor['venceu'] = True
            message = json.dumps(vencedor, ensure_ascii=False)
            ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='leilao_vencedor', body=message.encode("utf-8"))
        del leiloes_ativos[leilao_id]
        print(f" [x] Leilão encerrado: {leilao_id}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ----- Controle dos Lances -----
def callback_lances(ch, method, properties, body):
    body_str = body.decode('utf-8')
    #Lances são recebidos no formato: Lance(json string) + "||" + Assinatura
    json_part, assinatura_b64 = body_str.split("||")
    lance = json.loads(json_part)
    assinatura = base64.b64decode(assinatura_b64)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    #Se a assinatura confere, foi um lance em um evento ativo no momento e
    #não existem lances ou esse lance supera o valor anterior:
    if (verifica_assinatura(lance, assinatura) and
         lance['item'] in leiloes_ativos and
         (leiloes_ativos[lance['item']] is None or
          int(lance['valor']) > int(leiloes_ativos[lance['item']]['valor']))):

        #Cria o campo 'venceu', inicialmente com False
        lance['venceu'] = False
        leiloes_ativos[lance['item']] = lance
        print(f" [>] Lance válido recebido: Cliente {lance['id']} -> R$ {lance['valor']} no leilão {lance['item']}")
        message = json.dumps(lance, ensure_ascii=False)
        ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='lance_validado', body=message.encode("utf-8"))


# ----------

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
