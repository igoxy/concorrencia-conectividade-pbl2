# -*- coding: utf-8 -*-
from pathlib import Path
import socket
from Hidrometro import Hidrometro
from Servidor import Servidor
import threading
import random
import pickle
import sys


#Diretório
caminho_arquivo =  "./data/consumo.bin"

def carregar_dados() -> float:
    try: #Tenta abrir o arquivo que armazena o valor do consumo do usuário
        arquivo = open(caminho_arquivo, "rb")
        consumo = pickle.load(arquivo)
        arquivo.close()
        return consumo
    except : #Se o arquivo não existir, ou seja, se o cliente for novo, o consumo inicial será 0
        return 0.0

def gerar_vazao() -> float:
    """ Gera a vazão de acordo com o parêmtro passado """
    parametros = sys.argv[1:]
    if len(parametros) > 0:
        velocidade = str(parametros[0]).upper()
        if "LENTO" in velocidade:
            vazao = random.uniform(0.01, 0.33)  # Lento
        elif "RAPIDO" in velocidade:
            vazao = random.uniform(0.67, 1.0)   # Rápido
        else:                                      
            vazao = random.uniform(0.37, 0.66)  # Médio
    else:
        vazao = random.uniform(0.37, 0.66)      # Caso não tenha informado nenhum argumento válido
    
    return vazao

#Dados de entrada
matricula_h = str(input("Insira a matrícula do hidrômetro: "))
usuario = str(input("insira o nome do cliente relacionado ao hidrômetro: "))

#Criação dos objetos
hidrometro = Hidrometro(nome_usuario=usuario, vazao=gerar_vazao(), matricula=matricula_h, consumo=carregar_dados())
servidor = Servidor(hidrometro=hidrometro)

#Criação das Threads
thread_contabilizar_gasto = threading.Thread(target=hidrometro.calcular_consumo)
thread_enviar_dados = threading.Thread(target=hidrometro.enviar_dados)

#thread_servidor = threading.Thread(target=servidor.conectar)
#thread_servidor.daemon = True

#Inicialização das Threads
thread_contabilizar_gasto.start()
thread_enviar_dados.start()
#thread_servidor.start()

#Finalização das Threads
#hidrometro.stop()  #Para o envio de dados
#exit()