import socket
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

HOST = "127.0.0.1"
PORT = 5000

message = b'Ola, tudo bem?'

client_priv_key = RSA.import_key(open("./cliente/private_key.der", "rb").read())
h = SHA256.new(message)
assinatura = pkcs1_15.new(client_priv_key).sign(h)

with open("./cliente/signature.bin", "wb") as f:
    f.write(assinatura)

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   #
client_socket.connect((HOST, PORT))

client_socket.sendall(message + b"||" + assinatura)  #
client_socket.close()
