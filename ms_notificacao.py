import pika
import json


EXCHANGE_NAME = "leilao_control"


# ----- Controle dos Envios -----
#Recebe lances válidos e os envia para a fila do respectivo leilao:
#Lance {
#     "item": id do leilao,
#     "valor": valor do lance,
#     "id": id do cliente que fez o lance,
#     "venceu" True ou False (controlado por ms_lance)
# }
def callback(ch, method, properties, body):
    lance = json.loads(body.decode("utf-8"))

    if not lance['venceu']:
        print(f" [x] Mensagem recebida de lance_validado.")
    else:
        print(f" [v] Mensagem recebida de leilao_vencedor.")

    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=f"leilao_{lance['item']}", body=body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ----------

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

for queue in ['lance_validado', 'leilao_vencedor']:
    channel.queue_declare(queue=queue, exclusive=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue, routing_key=queue)
    channel.basic_consume(queue=queue, on_message_callback=callback)

print(' [*] Esperando novos lances.')
channel.start_consuming()
