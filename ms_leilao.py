import random
from datetime import datetime, timedelta
import pika
import json
import time


leiloes_init = [
    {
        "id": 1,
        "descricao": "Vaso cerimonial da era imperial Inca, ornamentado com símbolos solares",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id": 2,
        "descricao": "Capacete original do Darth Vader utilizado em filmagens de Star Wars",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id": 3,
        "descricao": "Manuscrito iluminado medieval do século XIII em pergaminho",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id": 4,
        "descricao": "Relógio de bolso em ouro 18k da família real britânica",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id": 5,
        "descricao": "Primeira edição de 'Dom Quixote' de Cervantes, encadernação em couro",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id": 6,
        "descricao": "Guitarra elétrica assinada por Jimi Hendrix",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id": 7,
        "descricao": "Máscara africana do povo Yoruba, esculpida em madeira nobre",
        "inicio": "",
        "fim": "",
        "status": ""
    }
]
EXCHANGE_NAME = "leilao_iniciado_ou_finalizado"
TEMPO_BASE = 10
FORMATO_TIME = "%Y-%m-%d %H:%M:%S"


#Gera horarios para inicio e fim de cada item da lista de leiloes
#e os retorna ordenados por ordem de inicio
def atualizar_datas(leiloes):
    agora = datetime.now()
    for leilao in leiloes:
        inicio = agora + timedelta(seconds=random.randint(0, TEMPO_BASE*2))
        fim = inicio + timedelta(seconds=random.randint(TEMPO_BASE, TEMPO_BASE*4))

        leilao["inicio"] = inicio.strftime(FORMATO_TIME)
        leilao["fim"] = fim.strftime(FORMATO_TIME)
    return sorted(leiloes, key=lambda x: datetime.strptime(x["inicio"], FORMATO_TIME))


def enviar_leilao(envio, RK):
    message = json.dumps(envio, ensure_ascii=False)
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=RK, body=message)
    print(f"\n [x] Leilao com ID: {envio['id']}, {RK}.", end='')


def iniciar(leiloes):
    print("LEILÃO INICIADO!!")
    while leiloes:
        agora = datetime.now()
        print(f"\rHorário: {agora.strftime('%H:%M:%S')}", end='', flush=True)

        pula_linha = False
        for leilao in leiloes[:]:
            inicio = datetime.strptime(leilao["inicio"], FORMATO_TIME)
            fim = datetime.strptime(leilao["fim"], FORMATO_TIME)

            if agora >= inicio and leilao["status"] == "":
                leilao["status"] = "ativo"
                enviar_leilao(leilao, "iniciado")
                pula_linha = True

            if agora >= fim and leilao["status"] == "ativo":
                leilao["status"] = "encerrado"
                enviar_leilao(leilao, "finalizado")
                leiloes.remove(leilao)
                pula_linha = True
        if pula_linha:
            print()
        time.sleep(1)


leiloes = atualizar_datas(leiloes_init)
for item in leiloes:
    print(item["id"], item["inicio"], item["fim"])

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")
channel.queue_declare(queue="leilao_iniciado")
channel.queue_bind(exchange=EXCHANGE_NAME, queue="leilao_iniciado", routing_key="iniciado")
channel.queue_declare(queue="leilao_finalizado")
channel.queue_bind(exchange=EXCHANGE_NAME, queue="leilao_finalizado", routing_key="finalizado")

iniciar(leiloes)
connection.close()
