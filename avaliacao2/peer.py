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
pedidos = []                            # fila com os pedidos para receber o recurso
pedidos_lock = threading.Lock()         # mutex para acessar a fila acima
status_holder = "RELEASED"              # status atual
status_lock = threading.Lock()          # mutex para acessar o valor acima
liberar = threading.Event()             # avisa a thread usando o recurso que deve finalizar e liberar o recurso
liberado = threading.Event()            # segura a thread principal até a liberação do recurso acontecer
proximo_na_lista = threading.Event()    # recurso livre para o próximo usar
saiu = threading.Event()                # alguem saiu da SC
console = Console()                     # para print da interface

# ----- CONFIG -----
FORMAT_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
HEARTBEAT_INTERVAL = 1
CLEANUP_INTERVAL = 3
TIMEOUT_BEATS = 5
TIMEOUT_BROADCAST = 5.0
LIMITE_USO_RECURSO = 30
# ------------------

# ----- Utilitários -----
def set_status(op):
    global status_holder
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
        status_holder = novo


def get_status():
    with status_lock:
        return status_holder


def peer_que_possui_recurso():
    with ativos_lock:
        for peer_name, info in peers_ativos.items():
            if info.get("status") == "HELD":
                return peer_name
    return None


def sou_o_mais_antigo(peer_name, client):
    with pedidos_lock:
        meu_pedido = next((t for t, p in pedidos if p == client), None)
        pedido_peer = (t for t, p in pedidos if p == peer_name)
    if meu_pedido is None:
        return False
    elif meu_pedido == pedido_peer:
        return client < peer_name
    return meu_pedido < pedido_peer


def proximo_pedido():
    with pedidos_lock:
        if pedidos:
            return pedidos[0][1]
        else:
            return None


def adicionar_pedido(peer_name, timestamp):
    with pedidos_lock:
        if any(p[1] == peer_name for p in pedidos):
            return
        pedidos.append((timestamp, peer_name))
        pedidos.sort(key=lambda x: x[0])


def remover_pedido(peer_name):
    global pedidos
    with pedidos_lock:
        pedidos = [p for p in pedidos if p[1] != peer_name]


# ----- Ações -----
def pedir_recurso(client):
    set_status('w')
    def solicitar_sc(client):
        timestamp = datetime.now().strftime(FORMAT_DATETIME)
        concedido = False
        while not concedido:
            concedido = broadcast_to_peers("requisitar", client, timestamp, timeout=True)
            if concedido:
                time.sleep(HEARTBEAT_INTERVAL)
                if peer_que_possui_recurso() is None and proximo_pedido() == client:
                    entrar_sc(client)
                    break
                else:
                    concedido = False
            proximo_na_lista.wait(timeout=LIMITE_USO_RECURSO)
    threading.Thread(target=solicitar_sc, args=(client,), daemon=True).start()


def entrar_sc(client):
    proximo_na_lista.clear()
    set_status('h')
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
    print("DONE!")
    sair_sc(client)
    if flag:
        liberado.set()


def sair_sc(client):
    print("Avisando peers que recurso está livre...")
    broadcast_to_peers("sair", client)
    saiu.wait(timeout=LIMITE_USO_RECURSO)
    set_status('r')
    proximo_na_lista = proximo_pedido()
    if proximo_na_lista is not None:
        Pyro5.api.Proxy(f"PYRONAME:{proximo_na_lista}").proximo()
    saiu.clear()


def liberar_recurso():
    tenho_recurso = get_status() == "HELD"
    if tenho_recurso:
        liberar.set()
        liberado.wait(timeout=TIMEOUT_BROADCAST)
        print("Recurso liberado!")
        liberar.clear()
        liberado.clear()
    else:
        print("Eu não tenho o recurso.")


# ----- Comunicação -----
def broadcast_to_peers(method_name, *args, timeout=False, check_ns=False):
    flag = True
    if check_ns:
        ns = Pyro5.api.locate_ns()
        lista = list(ns.list().keys())
        lista.remove("Pyro.NameServer")
    else:
        with ativos_lock:
            lista = peers_ativos.keys()
    for peer_name in lista:
        try:
            with Pyro5.api.Proxy(f"PYRONAME:{peer_name}") as peer:
                if timeout:
                    peer._pyroTimeout = TIMEOUT_BROADCAST
                try:
                    method = getattr(peer, method_name)
                    if not method(*args):
                        flag = False
                except Pyro5.errors.TimeoutError:
                    print(f"{peer} timeout no broadcast.")
                    with ativos_lock:
                        peers_ativos.pop(peer, None)
                    remover_pedido(peer)
        except:
            continue
    return flag


def peers_worker(client):
    @Pyro5.api.expose
    class ControlePeers(object):
        def requisitar(self, peer_name, timestamp):
            adicionar_pedido(peer_name, datetime.strptime(timestamp, FORMAT_DATETIME))
            if (get_status() == "HELD" or
                sou_o_mais_antigo(peer_name, client)):
                return False
            return True

        @Pyro5.api.oneway
        def sair(self, peer_name):
            remover_pedido(peer_name)
            if get_status() == "HELD":
                saiu.set()

        @Pyro5.api.oneway
        def proximo(self):
            proximo_na_lista.set()

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
    table.add_column("Request", justify="center", style="orange_red1")

    link = {}
    with pedidos_lock:
        for p in pedidos:
            link[p[1]] = p[0].strftime('%H:%M:%S.%f')

    with ativos_lock:
        for peer, info in peers_ativos.items():
            if peer not in link.keys():
                req = "-"
            else:
                req = link[peer]
            table.add_row(peer, info['status'], info['last_beat'], req)

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
    broadcast_to_peers("heartbeat", client, get_status(), datetime.now().strftime(FORMAT_DATETIME), check_ns=True)


def cleanup(client):
    with ativos_lock:
        for peer in list(peers_ativos.keys()):
            lb = datetime.strptime(peers_ativos[peer]['last_beat'], FORMAT_DATETIME)
            agora = datetime.now()
            if (agora - lb).total_seconds() > TIMEOUT_BEATS:
                peers_ativos.pop(peer, None)
                remover_pedido(peer)
                print(f"Peer removido por timeout: {peer}")


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

    # Thread para escutar peers
    threading.Thread(target=peers_worker, args=(client_name,), daemon=True).start()

    # Interface para exibição dos peers ativos e requisição/liberação do recurso
    interface_usuario(client_name)


if __name__ == "__main__":
    main()
