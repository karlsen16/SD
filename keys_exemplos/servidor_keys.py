import socket
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

HOST = "127.0.0.1"
PORT = 5000

client_public_key = RSA.import_key(open("./public_key.der", "rb").read())

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1) #

print("Servidor aguardando conexões...")
conn, addr = server_socket.accept()
print(f"Conexão recebida de {addr}")

data = conn.recv(4096)  #
message, assinatura = data.split(b"||")

h = SHA256.new(message)
try:
    pkcs1_15.new(client_public_key).verify(h, assinatura)
    print("Assinatura válida.")
except (ValueError, TypeError):
    print("Assinatura inválida")

conn.close()
server_socket.close()
