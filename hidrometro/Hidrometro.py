# -*- coding: utf-8 -*-

from paho.mqtt import client as mqtt_client
from datetime import datetime
from time import sleep
import socket
import pickle
import random
import json


class Hidrometro:

    #Constantes 
    HOST = 'broker.emqx.io'    # Endereço do broker
    PORT =  1883               # Porta do broker
    TOPIC = 'dados/hidrometro'
    CLIENTE_ID = f'python-mqtt-{random.randint(0, 1000)}'
    
    cliente = None
    #Diretório
    caminho_arquivo = "./data/consumo.bin"

    __vazao_padrao = 0.0
    
    #Atributos
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Objeto da conexão tcp

    __stop = 0 #Flag para indicar a parada das threads da geração de consumo e de envio de dados para o servidor

    def __init__ (self, nome_usuario: str, matricula: str, vazao: float = 0.0, consumo: float = 0.0, pressao: float = 1.0):
        self.__nome_usuario =    nome_usuario
        self.__matricula =      matricula
        self.__vazao =          vazao       # Vazão ao ligar o hidrômetro
        self.__consumo =        consumo
        self.__pressao =        pressao
        self.__vazao_padrao =   vazao       # Vazão padrão do hidrômetro
        
        self.__conectar()

    #Getters
    def get_nome_usuario(self) -> str:
        return self.__nome_usuario

    def get_matricula(self) -> str:
        return self.__matricula

    def get_vazao(self) -> float:
        return self.__vazao

    def get_consumo (self) -> float:
        return self.__consumo
    
    def get_pressao(self) -> float:
        return self.__pressao
    
    def get_vazao_padrao(self):
        return self.__vazao_padrao

    #Setters
    def set_nome_usuario(self, new_nome_usuario: str):
        self.__nome_usuario = new_nome_usuario
    
    def set_matricula(self, new_matricula: str):
        self.__matricula = new_matricula

    def set_vazao(self, new_vazao: float):
        self.__vazao = new_vazao

    def set_consumo(self, new_consumo: float):
        self.__consumo = new_consumo

    def set_pressao(self, nova_pressao: float):
        self.__pressao = nova_pressao

    def set_vazao_padrao(self, nova_vazao_padrao: float):
        self.__vazao_padrao = nova_vazao_padrao

    
    #Métodos funcionais 
    def stop(self):
        self.__stop = 1

    def enviar_dados(self) -> None:  #ENVIA TODOS OS DADOS
        """ Método para enviar os dados para o servidor """
        while True:
            if self.__stop == 1:
                break
            dados = self.__formatar_dados()
            self.__publish(dados=dados)
            sleep(5)  # Dorme por 5 segundos - Envia os dados de 5 em 5 segundos
        self.cliente.loop_stop()    #Para o cliente MQTT

    def calcular_consumo(self) -> None: #CALCULA O CONSUMO
        """ Método para calcular o consumo """
        while True:
            if self.__stop == 1: #Para a Thread de contabilizar 
                break   
            self.__consumo = round(self.__vazao + self.__consumo, 5) #Arredonda o valor da soma para 5 casas decimais
            self.__persistencia()
            print("Consumo atual: " + str(self.__consumo))
            sleep(1)
    
    
    #Métodos privados
    def __conectar(self):                               # Conectar com o servidor
        """ Método para efetuar a conexão """
        self.cliente = self.__connect_mqtt()
        self.cliente.loop_start()

    def __connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)

        client = mqtt_client.Client(self.CLIENTE_ID)
        client.on_connect = on_connect
        client.connect(self.HOST, self.PORT)
        return client
    
    def __publish(self, dados):
        """ Envia os dados para o servidor """
        result = self.cliente.publish(self.TOPIC, dados)
        status = result[0]
        if status == 0:
            print(f"Send `{dados}` to topic `{self.TOPIC}`")
        else:
            print(f"Failed to send message to topic {self.TOPIC}")

    def __formatar_dados(self) -> str:
        """ Método para organizar os dados """
        if (self.__pressao < 1 and self.__vazao == 0.0):    # Verifica se a vazão é igual a zero e a pressão está abaixo de 1 
            possivel_vazamento = True   # Sinaliza possível vazamento 
        else:
            possivel_vazamento = False
        dados = {                                                   # Organiza os dados para enviar para o servidor
            'dataHora': datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            'nomeUsuario': self.__nome_usuario,
            'matricula': self.__matricula,
            'consumo': self.__consumo,
            'vazao': self.__vazao,
            'possivelVazamento': possivel_vazamento,
            'host': str(socket.gethostbyname(socket.gethostname())),
            'port': 5035
        }

        json_dados = json.dumps(dados) #Cria o json para os dados

        return json_dados  # Retorna o dicionário em Json

    def __persistencia(self) -> None: #Método para persistir os dados
        """ Método para salvar o consumo na memória """
        try:
            arquivo = open(self.caminho_arquivo, "wb")  # Abre o arquivo
            pickle.dump(self.__consumo, arquivo)        # Adiciona o consumo ao arquivo
            arquivo.close()                             # Fecha o arquivo
        except Exception as ex:                         # Se ocorrer algum erro
            print("Erro ao gravar no arquivo. Causa: ", ex.args)

    