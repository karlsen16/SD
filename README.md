# Repositório de Avaliações

Este repositório reúne os trabalhos desenvolvidos em diferentes avaliações de Sistemas Distribuídos.  
Cada pasta (`avaliacao1`, `avaliacao2`, ...) contém seus próprios arquivos e dependências.

## Estrutura do Repositório
 - avaliacao1/   # Arquitetura Orientada a Eventos e Criptografia Assimétrica
   - Precisa ter o RabbitMQ instalado

 - avaliacao2/   # PyRO  

 - avaliacao3/   # TODO

 - avaliacao4/   # TODO  


## Dependências
Cada pasta (avaliacaoX/) pode ter seu próprio arquivo requirements.txt, listando apenas as dependências necessárias para aquele trabalho.

---
## Como utilizar

Para rodar qualquer uma delas, siga os passos:

1. **Entre na pasta da avaliação desejada**  
   ```bash
   cd avaliacao1   # ou avaliacao2, avaliacao3, etc.
2. **(Opcional) Crie e ative um ambiente virtual**
   ```bash
   python -m venv venv
   source venv/bin/activate   # Linux/macOS
   venv\Scripts\activate      # Windows
3. **Instale as dependências (requirements.txt dentro da pasta)**
   ```bash
   pip install -r requirements.txt
4. **Execute o programa**
   ```bash
   python3 nome_arquivo.py
