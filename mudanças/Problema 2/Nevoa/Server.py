#TODO MÉDIA DOS VALORES POR MEIO DO DATAFRAME SÓ DOS ÚLTIMOS VALORES DO CLIENTE - TESTAR
#TODO ENVIAR A MÉDIA PARA NUVEM
#TODO ATRIBUI UM NOME PARA CADA NEVOA
#TODO OBTER MÉDIA DA NUVEM
#TODO DESLIGAR CLIENTES QUE ULTRAPASSARAM A MÉDIA RECEBIDA DA NUVEM
#TODO FAZER UM COPIA DO DATAFRAME DOS ULTIMOS CONSUMOS PARA DESLIGAR OS CLIENTES DE ACORDO COM A MÉDIA
#TODO SALVAR LISTA DE CLIENTES BLOQUEADOS EM ARQUIVO - ESSES SÃO CLIENTES BLOQUEADOS POR MEIO DO CONSUMO DE ÁGUA EXAGERADO.
#TODO SALVAR TODOS OS DADOS SENSIVEIS EM ARQUIVOS
#TODO FAZER LOOP DE CONEXÃO
# ------ TESTES ------
# TODO TESTAR O BLOQUEIO DE CLIENTES POR MÉDIA
# TODO TESTAR O DESBLOQUEIO DE CLIENTE POR MÉDIA E REMOÇÃO DOS CLIENTES DA LISTA DE CLIENTES BLOQUEADOS
# TODO TESTAR O CALCULO DA MÉDIA
# TODO TESTAR O ENVIO DA MÉDIA PARA A NUVEM 
# TODO TESTAR O RECEBIMENTO DA MÉDIA DA NUVEM 
# TODO VERIFICAR SE REALMENTE O CLIENTE É VERIFICADO SE ESTA NA LISTA DE CLIENTES BLOQUEADOS

import json
import math
import pickle
import random
from time import sleep
from paho.mqtt import client as mqtt_client
import pandas as pd
from pandas import DataFrame
import numpy as np


class Server:
    #--------------------------------------------------------
    LIGAR =     "2"         # Constante para o comando de ligar um hidrômetro
    DESLIGAR =  "1"         # Constante para o comando de desligar um hidrômetro
    #---------------------------------------------------------
    #HOST_NUVEM = ''
    #PORT_NUVEM = ''
    #TOPIC_NUVEM = ''
    TOPIC_NUVEM_MEDIA_ENVIAR = 'nuvem/dados/media/nevoa'
    TOPIC_NUVEM_MEDIA_RECEBER = 'nuvem/dados/media/total'
    TOPIC_NUVEM_TEMPO_VAZAO = 'nuvem/dados/tempo-vazao'

    TOPIC_SERVIDOR = 'dados/hidrometro/servidor'
    TOPIC_HIDROMETRO = 'dados/hidrometro/nevoa'
    
    TOPICS = [(TOPIC_NUVEM_MEDIA_ENVIAR, 0), (TOPIC_NUVEM_MEDIA_RECEBER, 0), (TOPIC_NUVEM_TEMPO_VAZAO, 0), (TOPIC_SERVIDOR, 0), (TOPIC_HIDROMETRO, 2)]
    #----------------------------------------------------------
    HOST = 'broker.emqx.io'    # Endereço do broker
    PORT =  1883               # Porta do broker
    
    CLIENTE_ID = f'python-mqtt-{random.randint(0, 1000)}'
    #--------------------------------------------------
    
    #---------------------------------------------------------
    NAO_HA_MEDIA = -1       # Indica que ainda não há valores de consumo dos clientes na nevoa
    #---------------------------------------------------------
    cliente = None
    #---------------------------------------------------------
    __clientes_valores_media = None     #Armazena os valores e clientes incluidos na última média
    __lista_clientes_bloqueados = None  # Lista de clientes bloqueados por conusmo
    #---------------------------------------------------------
    pool_threads = []
    #-----------------------------------------------------
    __vazao_bloqueio = -1.0     # Inicia negativo, indica que não há um limite estabelecido de tempo e consumo para suspender o fornecimento de água de um cliente

    def __init__(self, consumo_clientes, identificacao_nevoa):
        self.start()
        self.__consumo_clientes = consumo_clientes
        self.identificacao_nevoa = identificacao_nevoa
        self.__carregar_dados()

    def start(self):
        """ Inicia o servidor """
        self.cliente = self.__connect_mqtt()
        self.__subscribe(self.cliente)
        self.cliente.loop_start()
    
    def calcular_media(self):
        while True:
            try:
                self.__clientes_valores_media = self.__consumo_clientes     # Faz uma cópia dos clientes e dos seus últimos valores de consumo utilizados para calcular a média
                media = self.__consumo_clientes['consumo'].mean()           # Obtém a média do consumo dos clientes
                if math.isnan(media):   # Verifica se há uma média de valores válida, ou seja, há pelo menos o valor de consumo de um cliente no sistema
                    media = self.NAO_HA_MEDIA   # Atribui um valor negativo 
                
                print(f'a média calcula foi: {media}')
                self.__enviar_media(media, self.identificacao_nevoa)      # Envia a média para a nuvem   
            except:
                print("Erro ao calcular a média.")  
            sleep(60)                                           # Dorme por 60 segundos          
        
# --------- Métodos privados ----------
    def __armazenar_dados(self, dados: dict):
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
            dados['consumoFatura'] = self.__consumo_fatura(None, dados, True)       # Se não existir um arquivo de dados associado ao cliente (matricula)
            dados['contaGerada'] = False 
            dados['pago'] = "-"
            dados['valor'] = self.__gasto_fatura(None, dados, 1.0, True)            # Adiciona as informações se a conta foi gerada a partir dessa leitura e se está paga
            self.__alerta_vazamento(dados)                                          # Verifica se há um alerta de possível vazamento do endereço recebido
            df = self.__inserir_dados(None, dados, True)                            # Cria um novo dataframe                           
            df.to_csv(f"{dados['matricula']}.csv", sep=',', index=False)            # Cria um arquivo csv com os dados  
        except Exception as ex:                                                     # Caso ocorra algum erro inesperado
            print("Erro ao armazenar dados. Causa: ", ex.args)                      # Informa o erro
        
        self.__verificar_tempo_vazao()                                              # Verifica se o cliente está consumindo acima do limite definido em um intervalo de tempo 

    def __inserir_dados(self, df: DataFrame, dados: dict, primeiro_valor: bool):
        """ Método responsável por inserir os dados no dataframe """
        try:
            self.__add_ultimo_consumo(str(dados['matricula']).zfill(3), dados['consumo'], dados['vazao'])   # Adiciona o novo consumo a lista último consumo do cliente
            if primeiro_valor:                                                              # Se for o primeiro valor a ser inserido no banco de dados
                df = pd.DataFrame(dados, index=[0])                                         # Cria um novo dataframe para os dados
                return df                                                                   # Retorna o dataframe
            else:                                                                           # Senão for o primerio dado a ser inserido no banco de dados
                nova_linha = pd.DataFrame(dados, index=[0])                                 # Cria a nova linha a ser inserida na tabela de dados                                       
                df = pd.concat([nova_linha, df[:]]).reset_index(drop=True)                  # Insere os dados recebidos no inicio da lista - Concatena
                return df                                                                   # Retorna o dataframe novo
            
        except Exception as ex:                                                             # Caso ocorra um erro ao adicionar o novo dado no dataframe
            print("Erro ao inserir dados no DataFrame. Causa", ex.args)                     
            return None                                                                     # Retorna um elemento None

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
    
    def __bloquear_hidrometro(self, matricula):
        """ Método responsável por bloquear um hidrômetro """
        try:
            if(self.__enviar_comando_hidrometro(self.DESLIGAR, matricula)):     #Envia o comando de desligar o hidrômetro
                print(F'O hidrômetro de matrícula {matricula} foi desligado!')
                return True
            else: 
                print(f'Não foi possível desligar o hidrômetro de matricula {matricula}!')
                return False                                                                  
        except Exception:
            print(f'Erro ao desligar o hidrômetro de matrícula {matricula}!')
            return False

    def __desbloquear_hidrometro(self, matricula):
        """ Método responsável por bloquear um hidrômetro """
        try:
            if(self.__enviar_comando_hidrometro(self.LIGAR, matricula)):     #Envia o comando de desligar o hidrômetro
                print(F'O hidrômetro de matrícula {matricula} foi ligado!')
                return True
            else: 
                print(f'Não foi possível ligar o hidrômetro de matricula {matricula}!')
                return False                                                                  
        except Exception:
            print(f'Erro ao ligar o hidrômetro de matrícula {matricula}!')
            return False
            
    def __enviar_comando_hidrometro(self, comando: str, matricula: str):
        """ Método responsável por enviar comandos para um hidrômetro """
        resultado = self.__publish(comando, f'hidrometro/acao/{matricula}')     # Envia o comando para o hidrômetro
        return resultado                                                        # Retorna o resultado da ação

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
            if (msg.topic == self.TOPIC_HIDROMETRO):
                dados_json = json.loads(msg.payload.decode("utf-8"))          # Converte a string no padrão Json em dicionário
                self.__armazenar_dados(dados_json)                       # Chama o método para o armazenamento dos dados recebidos
            elif (msg.topic == self.TOPIC_NUVEM_MEDIA_RECEBER):
                self.__media_recebida(float(msg.payload.decode("utf-8")))
            elif (msg.topic, msg.payload.decode('utf-8') == self.TOPIC_NUVEM_TEMPO_VAZAO):
                dados_json = json.loads(msg.payload.decode("utf-8"))
                self.__tempo_vazao(dados_json)
        
        client.subscribe(self.TOPIC)    #topic
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

    def __add_ultimo_consumo(self, matricula, consumo, vazao):
        """ Adiciona o último valor de consumo recebido do cliente pela nevoa ao dataframe"""
        try:
            self.__consumo_clientes.loc[self.__consumo_clientes['matricula'] == matricula, 'consumo'] =  consumo    # Altera o valor do consumo da matrícula informada
            self.__consumo_clientes.loc[self.__consumo_clientes['matricula'] == matricula, 'vazao'] =  vazao        # Altera o valor da vazão da matrícula informada
            self.__consumo_clientes.to_csv('consumo-clientes.csv', sep=',', index=False)
        except:
            print("Erro ao salvar o último valor de consumo do cliente.")
    
    def __enviar_media(self, media, identificacao, topic):
        """ Envia a média para a nuvem """
        dados = {'identificacao': identificacao, 'media': media}
        dados = json.dumps(dados)
        self.__publish(dados, topic)                # Envia a média para a nuvem

    def __media_recebida(self, media):
        """ Analisa a média recebida da nuvem e desliga os clientes que ultrapassaram """              
        lista_clientes_desligar =  self.__clientes_valores_media.loc[self.__clientes_valores_media['consumo'] > media].to_list('records')   # obtém a lista de clientes que tem o consumo acima da média - cada cliente ta como dicionário                                      
        self.__liberar_clientes(media)
        for cliente in lista_clientes_desligar:
            if (cliente not in self.__lista_clientes_bloqueados):       # Verifica se o cliente já não se encontra na lista de clientes bloqueados
                if (self.__bloquear_hidrometro(cliente['matricula'])):  # Obtém a matricula do cliente e envia o comando de desligar o hidrômetro referente aquela matricula
                    self.__lista_clientes_bloqueados.append(cliente)    # Adiciona o cliente a lista de clientes bloqueados

    def __liberar_clientes(self, media):
        """ Libera os clientes bloqueados que agora não estão acima da média de consumo """
        lista_auxiliar = []
        for cliente in self.__lista_clientes_bloqueados:    # Percorre a lista de clientes bloqueados
            if (cliente['consumo'] <= media):               # Verifica se o consumo do cliente é menor ou igual a média
                if (self.__desbloquear_hidrometro(cliente['matricula'])):   # Desbloqueia o cliente, caso atenda ao requisito da média
                    lista_auxiliar.append(cliente)                          # Adiciona o cliente desbloqueado a uma lista auxiliar para facilitar a remoção dos mesmos da lista de clientes bloqueados
        
        for item in lista_auxiliar:                                         # Percorre a lista auxiliar
            self.__lista_clientes_bloqueados.remove(item)                   # Remove os cliente desbloqueados da lista de clientes bloqueados

    def __tempo_vazao(self, dados: dict):
        tempo = int(dados['tempo'])             # Tempo em segundos
        consumo = float(dados['consumo'])       # m³
        self.__vazao_bloqueio = consumo/tempo   # Calcula a vazão de bloqueio de acordo com o tempo e o consumo fornecidos

    def __verificar_tempo_vazao(self):
        if (self.__vazao_bloqueio >= 0):    # Verifica se o valor de tempo e vazão para o desligamento de clientes foi estabelecido, se for negativo ainda não foi estabelecido, então não é possível desligar clientes com base nisso.
            lista_clientes_desligar = self.__clientes_valores_media.loc[self.__clientes_valores_media['vazao'] >= self.__vazao_bloqueio]    # obtém a lista de clientes que tem a vazão igual ou acima da de bloqueio - cada cliente ta como dicionário
            for cliente in lista_clientes_desligar:                             
                if (cliente not in self.__lista_clientes_bloqueados):           # Verifica se o cliente já não se encontra na lista de clientes bloqueados
                    if (self.__bloquear_hidrometro(cliente['matricula'])):      # Obtém a matricula do cliente e envia o comando de desligar o hidrômetro referente aquela matricula
                        self.__lista_clientes_bloqueados.append(cliente)        # Adiciona o cliente a lista de clientes bloqueados

    def __carregar_dados(self):
        caminho_arquivo = 'lista-clientes-bloqueados.bin'
        try:
            arquivo = open(caminho_arquivo, 'rb')
            self.__lista_clientes_bloqueados = pickle.load(arquivo)
            arquivo.close()
        except:     # Arquivo não existe, carrega a lista vazia
            self.__lista_clientes_bloqueados = []
            arquivo = open(self.caminho_arquivo, 'wb')                      # Abre o arquivo
            pickle.dump(self.__lista_clientes_bloqueados, arquivo)          # Adiciona a lista ao arquivo
            arquivo.close()                                                 # Fecha o arquivo
# ADICIONAR UM DATAFRAME PARA GUARDAR SOMENTE A MATRICULA E VALOR DO CONSUMO TOTAL DO CLIENTE - FACILITA A MÉDIA
    