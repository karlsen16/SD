import threading
import time
import Pyro5.api
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table


peers_ativos = {}                       # dict de dict {nome: {status, last_beat}}
ativos_lock = threading.Lock()          # mutex para acessar o dict acima
registro = threading.Event()            # sincroniza a primeira atribuição de "status" com o início dos heartbeats

esperando_por_peers = []                # fila dos peers que estou esperando resposta
espera_lock = threading.Lock()          # mutex para acessar a fila acima

respostas_pendentes = []                # fila dos peers que tenho que responder
respostas_lock = threading.Lock()       # mutex para acessar a fila acima

meu_status = "RELEASED"                 # status atual
status_lock = threading.Lock()          # mutex para acessar o valor acima

meu_pedido = None                       # timestamp do meu pedido mais antigo
pedido_lock = threading.Lock()          # mutex para acessar o valor acima

liberar = threading.Event()             # avisa a thread usando o recurso que deve finalizar e liberar o recurso
liberado = threading.Event()            # segura a thread principal até a liberação do recurso acontecer
minha_vez = threading.Event()           # lista de espera está vazia

console = Console()                     # para print da interface

# ----- CONFIG -----
FORMAT_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
HEARTBEAT_INTERVAL = 1
CLEANUP_INTERVAL = 3
TIMEOUT_BEATS = 5
TIMEOUT_ACAO = 10
LIMITE_USO_RECURSO = 30
# ------------------

# ----- Utilitários -----
def set_status(op):
    global meu_status
    if op == "r":
        novo = "RELEASED"
    elif op == "w":
        novo = "WANTED"
    elif op == "h":
        novo = "HELD"
    else:
        print("Erro ao alterar status.")
        return
    with status_lock:
        meu_status = novo


def get_status():
    with status_lock:
        return meu_status


def set_pedido(pedido=None):
    global meu_pedido
    with pedido_lock:
        if pedido is not None and meu_pedido is not None:
            return
        meu_pedido = pedido


def peer_que_possui_recurso():
    with ativos_lock:
        for peer_name, info in peers_ativos.items():
            if info.get("status") == "HELD":
                return peer_name
    return None


def sou_o_mais_antigo(client, peer_name, timestamp):
    with pedido_lock:
        pedido = meu_pedido
    if pedido is None:
        return False
    elif pedido == timestamp:
        return client < peer_name
    return pedido < timestamp


def adicionar_na_lista(peer_name, op):
    if op == "e":
        with espera_lock:
            if any(p == peer_name for p in esperando_por_peers):
                return
            esperando_por_peers.append(peer_name)
    elif op == "p":
        with respostas_lock:
            if any(p == peer_name for p in respostas_pendentes):
                return
            respostas_pendentes.append(peer_name)
    else:
        print("Erro ao adicionar nas listas.")


def remover_da_lista(peer_name, op):
    retorno = False
    if op == "a":
        with espera_lock:
            if peer_name in esperando_por_peers:
                esperando_por_peers.remove(peer_name)
        with respostas_lock:
            if peer_name in respostas_pendentes:
                respostas_pendentes.remove(peer_name)
    elif op == "e":
        with espera_lock:
            if peer_name in esperando_por_peers:
                esperando_por_peers.remove(peer_name)
            if esperando_por_peers:
                retorno = True
    elif op == "p":
        with respostas_lock:
            if peer_name in respostas_pendentes:
                respostas_pendentes.remove(peer_name)
            if respostas_pendentes:
                retorno = True
    else:
        print("Erro ao remover das listas.")
    return retorno


# ----- Ações -----
def pedir_recurso(client):
    set_status('w')
    def solicitar_sc(client):
        timestamp = datetime.now()
        set_pedido(timestamp)
        timestamp = timestamp.strftime(FORMAT_DATETIME)
        with ativos_lock:
            peers = list(peers_ativos.keys())
        if client in peers:
            peers.remove(client)

        concedido = False
        while not concedido:
            concedido = executar_acao("requisitar", peers, client, timestamp)
            if concedido:
                entrar_sc(client)
                break
            minha_vez.wait(timeout=LIMITE_USO_RECURSO*5)
    threading.Thread(target=solicitar_sc, args=(client,), daemon=True).start()


def entrar_sc(client):
    set_status('h')
    minha_vez.clear()
    set_pedido()
    print("Ganhei permissão para usar o recurso!")
    threading.Thread(target=usar_recurso, args=(client, LIMITE_USO_RECURSO,), daemon=True).start()


def usar_recurso(client, tempo):
    flag = False
    print(f"Usando recurso por {tempo}s:")
    for i in range(1, tempo+1):
        if liberar.is_set():
            flag = True
            break
        print("\r(" + "#" * i + "." * (tempo - i) + ")", flush=True, end="")
        time.sleep(1)
    print("\nDONE!")
    sair_sc(client)
    if flag:
        liberado.set()


def sair_sc(client):
    set_status('r')
    print("Avisando peers que recurso está livre...")
    with respostas_lock:
        lista = respostas_pendentes.copy()
    executar_acao("responder", lista, client)
    print("\nDONE!")


def liberar_recurso():
    tenho_recurso = get_status() == "HELD"
    if tenho_recurso:
        liberar.set()
        liberado.wait(timeout=TIMEOUT_ACAO)
        print("Recurso liberado!")
        liberar.clear()
        liberado.clear()
    else:
        print("Eu não tenho o recurso.")


# ----- Comunicação -----
def executar_acao(method_name, lista, *args):
    flag = True
    for peer_name in lista:
        try:
            with Pyro5.api.Proxy(f"PYRONAME:{peer_name}") as peer:
                peer._pyroTimeout = TIMEOUT_ACAO
                try:
                    method = getattr(peer, method_name)
                    if not method(*args):
                        flag = False
                        if method_name == "requisitar":
                            adicionar_na_lista(peer_name, 'e')
                    if method_name == "responder":
                        remover_da_lista(peer_name, 'p')
                except Pyro5.errors.TimeoutError:
                    print(f"{peer} timeout ao {method_name}.")
                    with ativos_lock:
                        peers_ativos.pop(peer, None)
                    if method_name == "requisitar":
                        remover_da_lista(peer_name, 'e')
        except:
            continue
    return flag


def peers_worker(client):
    @Pyro5.api.expose
    class ControlePeers(object):
        def requisitar(self, peer_name, timestamp):
            if (get_status() == "HELD" or
                sou_o_mais_antigo(client, peer_name, timestamp)):
                adicionar_na_lista(peer_name, 'p')
                return False
            return True

        @Pyro5.api.oneway
        def responder(self, peer_name):
            if not remover_da_lista(peer_name, 'e'):
                minha_vez.set()

        @Pyro5.api.oneway
        def heartbeat(self, peer_name, peer_status, timestamp):
            with ativos_lock:
                peers_ativos[peer_name] = {"status": peer_status, "last_beat": timestamp}

    ns = Pyro5.api.locate_ns()
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(ControlePeers)
    ns.register(client, uri)
    registro.set()

    print("\nEscutando peers..")
    daemon.requestLoop()


# ----- Interface -----
def mostrar_ativos(client):
    table = Table(title=f"{client} ::: Peers Ativos")

    table.add_column("ID", justify="center", style="cyan")
    table.add_column("Status", justify="center", style="green")
    table.add_column("Heartbeat", style="magenta")

    with ativos_lock:
        for peer, info in peers_ativos.items():
            table.add_row(peer, info['status'], info['last_beat'])
    console.print(table)

def interface_usuario(client):
    while True:
        print("\n=== MENU PEER ===")
        print("1. Listar peers ativos")
        print("2. Requisitar recursos")
        print("3. Liberar recursos")
        print("4. Sair")
        escolha = input("> ").strip()

        if escolha == "1":
            mostrar_ativos(client)

        elif escolha == "2":
            pedir_recurso(client)

        elif escolha == "3":
            liberar_recurso()

        elif escolha == "4":
            print("Saindo...")
            try:
                Pyro5.api.locate_ns().remove(client)
            except:
                pass
            break

        else:
            print("Opção inválida!")


# ----- Manutenção -----
def heart(client):
    registro.wait()
    ns = Pyro5.api.locate_ns()
    lista = list(ns.list().keys())
    lista.remove("Pyro.NameServer")
    executar_acao("heartbeat", lista, client, get_status(), datetime.now().strftime(FORMAT_DATETIME))


def cleanup(client):
    with ativos_lock:
        lista = {peer: peers_ativos[peer]['last_beat'] for peer, data in peers_ativos.items()}
    agora = datetime.now()
    for nome_peer, last_beat in lista.items():
        last_beat_obj = datetime.strptime(last_beat, FORMAT_DATETIME)
        if (agora - last_beat_obj).total_seconds() > TIMEOUT_BEATS:
            with ativos_lock:
                peers_ativos.pop(nome_peer, None)
            remover_da_lista(nome_peer, 'a')
            print(f"Peer removido por timeout: {nome_peer}")
            minha_vez.set()
            time.sleep(0.2)
            minha_vez.clear()


# ----- Inicialização e Main -----
def iniciar_nameserver_local():
    try:
        ns = Pyro5.api.locate_ns()
        print("NameServer encontrado.")
        for name, uri in list(ns.list().items()):
            if name != "Pyro.NameServer":
                try:
                    with Pyro5.api.Proxy(uri) as peer:
                        peer._pyroTimeout = 2
                        peer._pyroBind()
                except Pyro5.errors.CommunicationError:
                    ns.remove(name)
                    print(f"Registro removido do NameServer: {name} (peer inativo)")
                except Exception as e:
                    ns.remove(name)
                    print(f"Erro ao verificar {name}: {e}")
        print("Verificação de registros fantasmas concluída.")
    except Pyro5.errors.NamingError:
        print("Nenhum NameServer ativo encontrado. Criando um local...")

        def ns_thread():
            Pyro5.nameserver.start_ns_loop()

        threading.Thread(target=ns_thread, daemon=True).start()
        time.sleep(1)
        print("NameServer local iniciado.")


def main():
    iniciar_nameserver_local()
    client_name = f"peer{input('Digite seu ID: ')}.peers"

    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(heart, 'interval', seconds=HEARTBEAT_INTERVAL, args=[client_name], max_instances=1)
    scheduler.add_job(cleanup, 'interval', seconds=CLEANUP_INTERVAL, args=[client_name], max_instances=1)

    threading.Thread(target=peers_worker, args=(client_name,), daemon=True).start()

    interface_usuario(client_name)

if __name__ == "__main__":
    main()
