import threading
import time
import sys
import Pyro5.api
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table


peers_ativos = {}               # dict de dict {nome: {status, last_beat}}
ativos_lock = threading.Lock()  # mutex para acessar o dict acima
registro = threading.Event()    # sincroniza a primeira atribuição de "status" com o início dos heartbeats
pedidos = []                    # fila com os pedidos para receber o recurso
pedidos_lock = threading.Lock() # mutex para acessar a fila acima
liberar = threading.Event()     # avisa a thread usando o recurso que deve finalizar e liberar o recurso
liberado = threading.Event()    # segura a thread principal até a liberação do recurso acontecer
saiu_da_sc = threading.Event()  # recurso livre para o próximo usar
console = Console()             # para prints da interface

# ----- CONFIG -----
FORMAT_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
HEARTBEAT_INTERVAL = 1
CLEANUP_INTERVAL = 3
TIMEOUT_BEATS = 5
TIMEOUT_BROADCAST = 5.0
LIMITE_USO_RECURSO = 30
# ------------------

# ----- Utilitários -----
def set_status(client, op):
    with ativos_lock:
        if client not in peers_ativos:
            peers_ativos[client] = {}
        try:
            if op == "r":
                peers_ativos[client]["status"] = "RELEASED"
            elif op == "w":
                peers_ativos[client]["status"] = "WANTED"
            elif op == "h":
                peers_ativos[client]["status"] = "HELD"
        except:
            print("Erro no set de status.")


def peer_que_possui_recurso():
    with ativos_lock:
        for peer_name, info in peers_ativos.items():
            if info.get("status") == "HELD":
                return peer_name
    return None


def sou_o_mais_antigo(peer_name, my_name):
    with pedidos_lock:
        my_req = next((t for t, p in pedidos if p == my_name), None)
        other_req = next((t for t, p in pedidos if p == peer_name), None)

    if other_req is None:
        return True

    if my_req is None:
        return False

    return my_req < other_req


def adicionar_pedido(peer_name):
    timestamp = datetime.now()
    with pedidos_lock:
        if any(p[1] == peer_name for p in pedidos):
            return

        pedidos.append((timestamp, peer_name))
        pedidos.sort(key=lambda x: x[0])


def remover_pedido(peer_name):
    global pedidos
    with pedidos_lock:
        pedidos = [p for p in pedidos if p[1] != peer_name]


def usar_recurso(client, tempo):
    flag = False
    print(f"Usando recurso por {tempo}s:")
    for i in range(1, tempo+1):
        if liberar.is_set():
            flag = True
            break
        time.sleep(1)
        print("\r(" + "#" * i + "." * (tempo - i) + ")", flush=True, end="")
    set_status(client, 'r')
    broadcast_to_peers("sair", client)
    print("DONE!")
    if flag:
        liberado.set()


def broadcast_to_peers(method_name, *args, timeout=False):
    ns = Pyro5.api.locate_ns()
    flag = True
    for name in ns.list().keys():
        if name != "Pyro.NameServer":
            try:
                with Pyro5.api.Proxy(f"PYRONAME:{name}") as peer:
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
                flag = False
    return flag


# ----- Threads para escutar Peers Ativos -----
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


def peers_worker(client):
    @Pyro5.api.expose
    class ControlePeers(object):
        def requisitar(self, peer_name):
            adicionar_pedido(peer_name)
            if (peer_que_possui_recurso() == client or
                sou_o_mais_antigo(peer_name, client)):
                return False
            return True

        @Pyro5.api.oneway
        def sair(self, peer_name):
            remover_pedido(peer_name)
            saiu_da_sc.set()
            time.sleep(1)
            saiu_da_sc.clear()

        @Pyro5.api.oneway
        def heartbeat(self, peer_name, peer_status, timestamp):
            with ativos_lock:
                peers_ativos[peer_name] = {"status": peer_status, "last_beat": timestamp}

    ns = Pyro5.api.locate_ns()
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(ControlePeers)
    ns.register(client, uri)
    with ativos_lock:
        peers_ativos[client] = {"status": "RELEASED", "last_beat": datetime.now().strftime(FORMAT_DATETIME)}
    registro.set()

    print("\nEscutando peers..")
    daemon.requestLoop()


# ----- Threads para realizar Heartbeats e Timeouts -----
def heart(client):
    registro.wait()
    with ativos_lock:
        status_atual = peers_ativos[client]["status"]
    broadcast_to_peers("heartbeat", client, status_atual, datetime.now().strftime(FORMAT_DATETIME))


def cleanup(client):
    with ativos_lock:
        for peer in list(peers_ativos.keys()):
            lb = datetime.strptime(peers_ativos[peer]['last_beat'], FORMAT_DATETIME)
            agora = datetime.now()
            if (agora - lb).total_seconds() > TIMEOUT_BEATS:
                del peers_ativos[peer]
                remover_pedido(peer)
                print(f"Peer removido por timeout: {peer}")


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
            set_status(client, 'w')
            def solicitar_sc(client):
                concedido = False
                print("Tentando conseguir recurso", end="")
                concedido = broadcast_to_peers("requisitar", client, timeout=True)
                while not concedido:
                    saiu_da_sc.wait(timeout=TIMEOUT_BROADCAST + 2)
                    concedido = broadcast_to_peers("requisitar", client, timeout=True)

                set_status(client, 'h')
                print("Ganhei permissão para usar o recurso!")
                t3 = threading.Thread(target=usar_recurso, args=(client, LIMITE_USO_RECURSO,), daemon=True)
                t3.start()

            t2 = threading.Thread(target=solicitar_sc, args=(client,), daemon=True)
            t2.start()

        elif escolha == "3":
            with ativos_lock:
                tenho_recurso = peers_ativos[client]['status'] == "HELD"
            if tenho_recurso:
                liberar.set()
                liberado.wait(timeout=TIMEOUT_BROADCAST + 2)
                print("Recurso liberado!")
                liberar.clear()
                liberado.clear()
            else:
                print("Eu não tenho o recurso.")

        elif escolha == "4":
            print("Saindo...")
            try:
                Pyro5.api.locate_ns().remove(client)
            except:
                pass
            break

        else:
            print("Opção inválida!")


# ----- Main -----
def main():
    iniciar_nameserver_local()
    client_name = f"peer{input('Digite seu ID: ')}.peers"

    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(heart, 'interval', seconds=HEARTBEAT_INTERVAL, args=[client_name], max_instances=1)
    scheduler.add_job(cleanup, 'interval', seconds=CLEANUP_INTERVAL, args=[client_name], max_instances=1)

    # Thread para escutar peers
    t1 = threading.Thread(target=peers_worker, args=(client_name,), daemon=True)
    t1.start()

    # Interface para exibição dos peers ativos e requisição/liberação do recurso
    interface_usuario(client_name)


if __name__ == "__main__":
    main()
