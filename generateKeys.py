from Crypto.PublicKey import RSA

#cria um par de chaves
key = RSA.generate(2048)

private_key = key.export_key(format='DER')
with open("./cliente/private_key.der", "wb") as f:
    f.write(private_key)

public_key = key.publickey().export_key(format='DER')
with open("./cliente/public_key.der", "wb") as f:
    f.write(public_key)
