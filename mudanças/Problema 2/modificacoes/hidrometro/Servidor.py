# -*- coding: utf-8 -*-
import socket
from paho.mqtt import client as mqtt_client
from Hidrometro import Hidrometro
import random


class Servidor:

    #CONSTANTES
    #HOST = ''              # Endereco IP do Servidor
    #PORT = 5035            # Porta que o Servidor está ouvindo

    HOST = 'broker.emqx.io'    # Endereço do broker
    PORT =  1883               # Porta do broker
    TOPIC = ''
    CLIENTE_ID = f'python-mqtt-{random.randint(0, 1000)}'

    #Flag de parada do servidor
    parar = 0

    def __init__(self, hidrometro: Hidrometro):
        
        self.__hidrometro = hidrometro
        self.TOPIC = (f'hidrometro/acao/{(str(self.__hidrometro.get_matricula())).zfill(3)}')    #Tópico para receber comandos
        self.__conectar()
    
    def conectar(self):
        """ Método para conectar o servidor do hidrômetro com o servidor de dados para o recebimento de informações do servidor """
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        orig = (self.HOST, self.PORT)
        tcp.bind(orig)
        tcp.listen(1)
        while True:
            try:
                con, cliente = tcp.accept()
                
                print ("Concetado por", cliente)
                while True:
                    msg = con.recv(1024)
                    if not msg: 
                        break
                    print (cliente, ": ", msg.decode("utf-8"))

                    if msg.decode("utf-8") == "1": #1 - indica deve desativar o cliente, ou seja, o fornecimento de água foi suspenso
                        self.__hidrometro.set_vazao(0)  # Seta a vazão para zero
                    elif msg.decode("utf-8") == "2":   # Libera o fornecimento de água
                        self.__hidrometro.set_vazao(self.__hidrometro.get_vazao_padrao())   # Liga o hidrômetro com a vazão padrão do mesmo
                print ("Finalizando conexao do cliente", cliente)
                con.close()
            except:
                print("Erro na conexão")
    
    def stop(self):
        """ Método para parar o servidor do hidrômetro """
        self.parar = 1
    
    #Métodos privados
    def __conectar(self):                               # Conectar com o servidor
        """ Método para efetuar a conexão """
        self.cliente = self.__connect_mqtt()
        self.__subscribe(self.cliente)
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
    
    def __subscribe(self, client: mqtt_client):
        def on_message(client, userdata, msg):
            print(f"Received {msg.payload.decode('utf-8')} from {msg.topic} topic")
            if (msg.topic == self.TOPIC):            
                if msg.payload.decode('utf-8') == "1": #1 - indica deve desativar o cliente, ou seja, o fornecimento de água foi suspenso
                    self.__hidrometro.set_vazao(0)  # Seta a vazão para zero
                elif msg.payload.decode('utf-8') == "2":   # Libera o fornecimento de água
                    self.__hidrometro.set_vazao(self.__hidrometro.get_vazao_padrao())   # Liga o hidrômetro com a vazão padrão do mesmo
        
        client.subscribe(self.TOPIC, qos=2)    #topic
        client.on_message = on_message