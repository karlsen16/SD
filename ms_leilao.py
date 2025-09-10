import random
from datetime import datetime, timedelta
import pika
import json
import time
import os
from rich.console import Console
from rich.table import Table

leiloes_init = [
    {
        "id_leilao": "1",
        "descricao": "Vaso cerimonial da era imperial Inca, ornamentado com símbolos solares",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id_leilao": "2",
        "descricao": "Capacete original do Darth Vader utilizado em filmagens de Star Wars",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id_leilao": "3",
        "descricao": "Manuscrito iluminado medieval do século XIII em pergaminho",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id_leilao": "4",
        "descricao": "Relógio de bolso em ouro 18k da família real britânica",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id_leilao": "5",
        "descricao": "Primeira edição de 'Dom Quixote' de Cervantes, encadernação em couro",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id_leilao": "6",
        "descricao": "Guitarra elétrica assinada por Jimi Hendrix",
        "inicio": "",
        "fim": "",
        "status": ""
    },
    {
        "id_leilao": "7",
        "descricao": "Máscara africana do povo Yoruba, esculpida em madeira nobre",
        "inicio": "",
        "fim": "",
        "status": ""
    }
]
EXCHANGE_NAME = "leilao_control"
TEMPO_BASE = 10
FORMATO_TIME = "%Y-%m-%d %H:%M:%S"
console = Console()


# ----- Interface do Terminal -----
def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')


def mostrar_leiloes():
    clear_terminal()
    table = Table(title="Leilões")

    table.add_column("ID", justify="center", style="cyan", no_wrap=True)
    table.add_column("Inicio", justify="center", style="green")
    table.add_column("Fim", style="magenta")
    table.add_column("Status", justify="right", style="yellow")

    for leilao in leiloes:
        table.add_row(leilao['id_leilao'], leilao['inicio'][-8:], leilao['fim'][11:19], leilao['status'])

    console.print(table)
    print(f"\rHorário: {datetime.now().strftime('%H:%M:%S')}", end='', flush=True)


# ----- Inicialização -----
#Gera tempos aleatórios para inicio e fim dos leiloes
#com base no horário atual
def atualizar_datas(leiloes):
    agora = datetime.now()
    for leilao in leiloes:
        inicio = agora + timedelta(seconds=random.randint(0, TEMPO_BASE*2))
        fim = inicio + timedelta(seconds=random.randint(TEMPO_BASE, TEMPO_BASE*4))

        leilao["inicio"] = inicio.strftime(FORMATO_TIME)
        leilao["fim"] = fim.strftime(FORMATO_TIME)
    return sorted(leiloes, key=lambda x: datetime.strptime(x["inicio"], FORMATO_TIME))


# ----- Controle dos Envios -----
def enviar_leilao(leilao, RK):
    message = json.dumps(leilao, ensure_ascii=False)
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=RK, body=message.encode("utf-8"))
    print(f"\n [x] Leilao com ID: {leilao['id_leilao']} enviado para {RK}.", end='')


def iniciar(leiloes):
    mostrar_leiloes()
    try:
        while leiloes:
            agora = datetime.now()
            print(f"\rHorário: {agora.strftime('%H:%M:%S')}", end='', flush=True)

            for leilao in leiloes[:]:
                inicio = datetime.strptime(leilao["inicio"], FORMATO_TIME)
                fim = datetime.strptime(leilao["fim"], FORMATO_TIME)

                if agora >= inicio and leilao['status'] not in ["ativo", "encerrado"]:
                    leilao["status"] = "ativo"
                    enviar_leilao(leilao, "leilao_iniciado")
                    mostrar_leiloes()

                if agora >= fim and leilao['status'] == "ativo":
                    leilao["status"] = "encerrado"
                    enviar_leilao(leilao, "leilao_finalizado")
                    leiloes.remove(leilao)
                    mostrar_leiloes()
            time.sleep(1)
    finally:
        for leilao in leiloes:
            if leilao['status'] == "ativo":
                leilao['status'] = "encerrado"
                enviar_leilao(leilao, "leilao_finalizado")
        connection.close()
        print("\n [*] Conexão com RabbitMQ encerrada.")


# ----------

leiloes = atualizar_datas(leiloes_init)
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")

if "TERM" not in os.environ:
    os.environ["TERM"] = "xterm-256color"

iniciar(leiloes)
