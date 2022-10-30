# -*- coding: utf-8 -*-

import threading
from ReceiveServidor import ReceiveServidor
import pandas as pd
from ApiServidor import ApiServidor

def add_usuarios():
    """ Função para carregar os usuários do sistema """
    try:
        df = pd.read_csv('usuarios.csv', sep=',')
    except:
        """ Criar os usuários do sistema """
        usuarios = [['001', 'fulano1', '929.124.250-00', True, False, 'Rua A - Campo Limpo - 2', False],
                ['002', 'fulano2', '773.928.570-61', True, False, 'Rua B - Campo Limpo - 2', False],
                ['003', 'fulano3', '827.086.480-39', True, False, 'Rua C - Campo Limpo - 2', False],
                ['004', 'fulano4', '287.613.940-50', True, False, 'Rua D - Campo Limpo - 2', False]]
        df = pd.DataFrame(usuarios, columns=['matricula', 'nome', 'cpf', 'ativo', 'pendencia', 'endereco', 'possivelVazamento'])
        df.to_csv('usuarios.csv', sep=',', index=False)

def add_adms():
    """ Função para carregar os administradores do sistema """
    try:
        df = pd.read_csv('adms.csv', sep=',')
    except:
        """ Cria o administrador do sistema """
        adm = [['admin', 'admin']]
        df = pd.DataFrame(adm, columns=['login', 'senha'])
        df.to_csv('adms.csv', sep=',', index=False)

add_usuarios() # Criar os usuários do sistema
add_adms()    # Cria o administrador do sistema

#print(f"Endereço do servidor: {str(socket.gethostbyname(socket.gethostname()))}") # Mostra o IP que o servidor está hospedado

api = ApiServidor('', 5050)             # Cria o sevidor da API
servidor_api = threading.Thread(target=api.start) 
#servidor_api.daemon = True

servidor = ReceiveServidor()    # Cria o servidor para o recebimento de dados
#servidor_dados = threading.Thread(target=servidor.start)

servidor_api.start()
#servidor_dados.start()