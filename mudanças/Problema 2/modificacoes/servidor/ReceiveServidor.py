# TODO MULTIPLOS SUBCRIBE - MUDAR PARA SER ASSIM

# -*- coding: utf-8 -*-
from paho.mqtt import client as mqtt_client
import random
import pandas as pd
from pandas import DataFrame
import numpy as np
import socket
import json
import threading

# portas: 5006 a 5009

class ReceiveServidor():

    # ------- Tópicos --------
    TOPIC_MEDIA_NEVOA_RECEBER = 'nuvem/dados/media/nevoa'
    TOPIC_NEVOAS_CONECTADAS = 'nuvem/sistema/nevoas/conectada'
    TOPIC_NUVEM_MEDIA_ENVIAR = 'nuvem/dados/media/total'

    TOPICS = [(TOPIC_MEDIA_NEVOA_RECEBER, 2), (TOPIC_NEVOAS_CONECTADAS, 2), (TOPIC_NUVEM_MEDIA_ENVIAR, 2)]
    # ----- Constante do MQTT -----------
    HOST = 'broker.emqx.io'    # Endereço do broker
    PORT =  1883               # Porta do broker
    TOPIC = 'dados/hidrometro/servidor'
    CLIENTE_ID = f'python-mqtt-{random.randint(0, 1000)}'

    # ------- Cliente MQTT ----------
    cliente = None

    # ------- Listas e dicionários ---------------
    __list_nevoas_conectadas = []
    __medias_nevoas = {}

    def __init__(self):
        self.start()


    def start(self):
        """ Inicia o servidor """
        self.cliente = self.__connect_mqtt()
        self.__subscribe(self.cliente)
        self.cliente.loop_start()
        
# --------- Métodos privados ----------
    def __armazenarDados(self, dados: dict):
        """ Método responsável por armazenar os dados recebidos dos hidrômetros no banco """
        try:
            df = pd.read_csv(f"{dados['matricula']}.csv", sep=',')                  # Tenta abrir o arquivo csv relacionado a matricula
            dados['consumoFatura'] = self.__consumo_fatura(df, dados, False)        
            dados['valor'] = self.__gasto_fatura(df, dados, 1.0, False)
            dados['contaGerada'] = False
            dados['pago'] = "-"
            self.__alerta_vazamento(dados)                                          # Verifica se há um alerta de possível vazamento do endereço recebido
            df = self.__inserir_dados(df, dados, False)                             # Adiciona os dados recebidos do hidrômetro na base de dados                                            
            df.to_csv(f"{dados['matricula']}.csv", sep=',', index=False)            # Salva os dados em uma arquivo csv
        except FileNotFoundError:  
            dados['consumoFatura'] = self.__consumo_fatura (None, dados, True)      # Se não existir um arquivo de dados associado ao cliente (matricula)
            dados['contaGerada'] = False 
            dados['pago'] = "-"
            dados['valor'] = self.__gasto_fatura(None, dados, 1.0, True)            # Adiciona as informações se a conta foi gerada a partir dessa leitura e se está paga
            self.__alerta_vazamento(dados)                                          # Verifica se há um alerta de possível vazamento do endereço recebido
            df = self.__inserir_dados(None, dados, True)                            # Cria um novo dataframe                           
            df.to_csv(f"{dados['matricula']}.csv", sep=',', index=False)            # Cria um arquivo csv com os dados
        except Exception as ex:                                                     # Caso ocorra algum erro inesperado
            print("Erro ao armazenar dados. Causa: ", ex.args)                      # Informa o erro

    def __inserir_dados(self, df: DataFrame, dados: dict, primeiro_valor: bool):
        """ Método responsável por inserir os dados no dataframe """
        try:
            if primeiro_valor:                                                         # Se for o primeiro valor a ser inserido no banco de dados
                df = pd.DataFrame(dados, index=[0])                                    # Cria um novo dataframe para os dados
                return df                                                              # Retorna o dataframe
            else:                                                                      # Senão for o primerio dado a ser inserido no banco de dados
                nova_linha = pd.DataFrame(dados, index=[0])                            # Cria a nova linha a ser inserida na tabela de dados                                       
                df = pd.concat([nova_linha, df[:]]).reset_index(drop=True)             # Insere os dados recebidos no inicio da lista - Concatena
                return df                                                              # Retorna o dataframe novo
        except Exception as ex:                                                        # Caso ocorra um erro ao adicionar o novo dado no dataframe
            print("Erro ao inserir dados no DataFrame. Causa", ex.args)                     
            return None                                                                # Retorna um elemento None

    def __gasto_fatura(self, df: DataFrame, dados: dict, taxa: float, primeiro_valor: bool):
        """ Método responsável por fazer o calculo do gasto """
        if (primeiro_valor):                                                                    # Se for o primerio valor a ser inserido no banco de dados
            valor_gasto = float(dados['consumo'])*taxa
            return valor_gasto
        else:                                                                                   # Caso não seja o primerio valor a ser inserido no banco de dados
            medida_antiga = bool(df.iloc[0]['contaGerada'])
            if medida_antiga:
                valor_gasto = (float(dados['consumo']) - float(df.iloc[0]['consumo']))*taxa     # Encontra o valor gasto atual (caso o valor anterior tenha sido contabilizado para uma fatura anterior)
                return valor_gasto
            else:
                valor_gasto = ((float(dados['consumo'])) - (float(df.iloc[0]['consumo']))*taxa) + float(df.iloc[0]['valor'])  # Encontra o valor gasto atual (caso o valor anterior não tenha sido contabilizado para uma fatura anterior)
                return valor_gasto
    
    def __consumo_fatura(self, df: DataFrame, dados: dict, primeiro_valor: bool):
        """ Método responsável por computar o consumo da fatura atual do cliente """
        if (primeiro_valor):                                                                    # Se for o primerio consumo a ser inserido no banco de dados
            consumo_fatura = float(dados['consumo'])
            return consumo_fatura
        else:                                                                                   # Caso não seja o primerio consumo a ser inserido no banco de dados
            medida_antiga = bool(df.iloc[0]['contaGerada'])
            if medida_antiga:
                consumo_fatura = float(dados['consumo']) - float(df.iloc[0]['consumo'])         # Encontra o consumo atual (caso o valor anterior tenha sido contabilizado para uma fatura anterior)
                return consumo_fatura
            else:  
                consumo_fatura = (float(dados['consumo']) - float(df.iloc[0]['consumo'])) + float(df.iloc[0]['consumoFatura'])    # Encontra o consumo atual (caso o valor anterior não tenha sido contabilizado para uma fatura anterior)
                return consumo_fatura
    
    def __alerta_vazamento(self, dados: dict):
        """ Método responsável por identificar mensagens de possíveis vazamentos recebidas do hidrômetro """
        if (bool(dados['possivelVazamento'])):                                                  # Verifica se recebeu o alerta de possível vazamento
            df = pd.read_csv("usuarios.csv", sep=',')                                           # Abre o arquivo do banco de dados dos usuários
            indice_usuario = (df.index[df['matricula'] == int(dados['matricula'])]).tolist()[0] # Obtém o indice que refencia o usuário no dataframe de usuários
            df.at[indice_usuario, 'possivelVazamento'] = True                                   # Indica que há um possível vazamento no endereço do usuário
            df.to_csv("usuarios.csv", sep=',', index=False)                                     # Salva as informações no banco de dados csv
        else:
            df = pd.read_csv("usuarios.csv", sep=',')                                           # Abre o arquivo do banco de dados dos usuários
            indice_usuario = (df.index[df['matricula'] == int(dados['matricula'])]).tolist()[0] # Obtém o indice que refencia o usuário no dataframe de usuários
            df.at[indice_usuario, 'possivelVazamento'] = False                                  # Indica que há um possível vazamento no endereço do usuário
            df.to_csv("usuarios.csv", sep=',', index=False)                                     # Salva as informações no banco de dados csv
    
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

    def __subscribe(self, client: mqtt_client):
        def on_message(client, userdata, msg):
            print(f"Received {msg.payload.decode('utf-8')} from {msg.topic} topic")
            if (msg.topic == self.TOPIC):
                print(msg.topic, msg.payload.decode("utf-8"))               
                dados_json = json.loads(msg.payload.decode("utf-8"))          # Converte a string no padrão Json em dicionário
                self.__armazenarDados(dados_json)                             # Chama o método para o armazenamento dos dados recebidos
            elif (msg.topic == self.TOPIC_MEDIA_NEVOA_RECEBER):
                dados_json = json.loads(msg.payload.decode("utf-8"))          # Converte a string no padrão Json em dicionário
                self.__receber_media(dados_json)
            elif (msg.topic == self.TOPIC_NEVOAS_CONECTADAS):
                self.__nevoas_conectadas(msg.payload.decode("utf-8"))
        
        client.subscribe(self.TOPICS)    
        client.on_message = on_message
    
    def __publish(self, dados, topico):
        """ Envia os dados para o servidor """
        result = self.cliente.publish(topico, dados)
        status = result[0]
        if status == 0:
            print(f"Send `{dados}` to topic `{topico}`")
            return True
        else:
            print(f"Failed to send message to topic {topico}")
            return False

    def __receber_media(self, dados: dict):
        self.__medias_nevoas[dados['identificacao']] = dados['media']   # Atribui a média do nevoa a sua chave correspondente do dicionário

        self.__calcular_media()                                         # Chama a função de cálculo de média para identificar se já é possível calcular a média
    
    def __nevoas_conectadas(self, identificacao):
        """ Reconhece as nevoas conectadas a nuvem """
        if (identificacao not in self.__list_nevoas_conectadas):
            self.__list_nevoas_conectadas.append(identificacao)
            self.__medias_nevoas[identificacao] = np.NaN   # Adiciona a nevoa a lista de nevoas
    
    def __limpar_lista_nevoas_conectadas(self):
        """ Limpa a lista de clientes conectados - isso é feito para verificar novamente se todos as nevoas estã conectadas"""
        self.__list_nevoas_conectadas.clear()
    
    def __calcular_media(self):
        """ Efetua o calculo da média das médias recebidas das nevoas - esse calculo é feito apenas quando todos os dados forem recebidos de todas as nevoas """
        medias = self.__medias_nevoas.values()                  # Obtém a lista com a média de cada nevoa
        if (np.NaN not in medias):                              # Verifica se todas as médias foram recebidas das nevoas
            t_media = filter(lambda val: val >= 0, medias)      # Verifica se há elementos menor que 0 e remove, ou seja, elementos que indiquem que uma nevoa não tem media
            media = np.average(t_media)                         # Calcula a média das médias recebidas
            self.__enviar_media(media)
            for key in self.__medias_nevoas.keys():             # Remove as medias antigas para o cálculo de uma nova média
                self.__medias_nevoas[key] = np.NaN              # Atribui um valor não numérico 
    
    def __enviar_media(self, media):
        self.__publish(media, self.TOPIC_NUVEM_MEDIA_ENVIAR)