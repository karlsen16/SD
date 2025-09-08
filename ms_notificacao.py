import pika
import json


EXCHANGE_NAME = "leilao_control"


def callback(ch, method, properties, body):
    try:
        lance = json.loads(body.decode("utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if not lance['venceu']:
            print(f" [x] Mensagem recebida de lance_validado:\n"
                  f" Cliente ({lance['id']}) deu lance de valor {lance['valor']}\n"
                  f" no item de id {lance['item']}!")
        else:
            print(f" [x] Mensagem recebida de leilao_vencedor:\n"
                  f" PARABÉNS Cliente {lance['id']}!! Você venceu o leilao do item {lance['item']}!\n"
                  f" O valor {lance['valor']} foi o mais alto e garantiu o arremate.")
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=f"leilao_{lance['item']}", body=body)
    except Exception as e:
        print(f"\n [!] Erro ao enviar lance {lance['item']} -> {e}")


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

for queue in ['lance_validado', 'leilao_vencedor']:
    channel.queue_declare(queue=queue, exclusive=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue, routing_key=queue)
    channel.basic_consume(queue=queue, on_message_callback=callback)

print(' [*] Esperando novos lances.')
channel.start_consuming()
