# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta
import json
import os
import re
import socket
import pandas as pd
from pandas import DataFrame
import threading

class ApiServidor():
    """ Classe do servidor da API """
    # ---------- Mensagens ----------
    NOT_FOUND =             ('404')
    NOT_ACCEPTABLE =        ('406')
    OK =                    ('200')
    INTERNAL_SERVER_ERROR = ('500')
    NOT_IMPLEMENTED =       ('501')

    # ---------- Rotas ----------
    ADM_GERAR_FATURA = '/ADMINISTRADOR/GERAR_FATURA'            # POST  -   OK
    ADM_VAZAMENTOS = '/ADMINISTRADOR/VAZAMENTOS'                # GET   -   OK
    ADM_DESLIGAR_CLIENTE = '/ADMINISTRADOR/DESLIGAR_CLIENTE'    # POST  -   OK
    SYS_RECEBER_PAGAMENTO = '/PAGAMENTO/RECEBER'                # POST  -   OK
    USER_CONSUMO_DATA_HORARIO = '/CLIENTE/CONSUMO/DATA_HORARIO' # GET   -   OK
    USER_CONSUMO_TOTAL = '/CLIENTE/CONSUMO/TOTAL'               # GET   -   OK
    USER_CONSUMO_ATUAL = '/CLIENTE/CONSUMO/FATURA_ATUAL'        # GET   -   OK
    USER_OBTER_FATURA = '/CLIENTE/OBTER_FATURA'                 # GET   -   OK
    LISTAR_CLIENTES = '/LISTAR_CLIENTES'                        # GET   -   OK 
    LOGIN_ADM = '/LOGIN_ADM'                                    # GET   -   OK
    LOGIN_USUARIO = '/LOGIN_USUARIO'                            # GET   -   OK

    __colecao_threads = {}              # Pool de threads

    def __init__(self, host: str, port: int):
        self.__host = host
        self.__port = port
        self.__server_socket_tcp = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)  # Criando o socket

    def start(self):
        """ Inicia o servidor da API """
        endpoint = (self.__host, self.__port)
        try:
            self.__server_socket_tcp.bind(endpoint)
            self.__server_socket_tcp.listen(1)
            print(f'Servidor da API iniciado na porta: {self.__port}')
            while True:
                # Esperando conexão do cliente
                conexao, cliente = self.__server_socket_tcp.accept()
                self.__colecao_threads[cliente] = threading.Thread(
                    target=self.__requisicao, args=([conexao]))
                self.__colecao_threads[cliente].daemon = True
                self.__colecao_threads[cliente].start()
        except Exception as ex:
            print("Erro no servidor", ex.args)
            self.__server_socket_tcp.close()

    def __requisicao(self, conexao):
        """ Trata as requisições do recebidas no servidor da API"""
        def enviar_resposta(con, resposta):
            """Método para envia a resposta ao cliente"""
            try:
                con.sendall(resposta.encode())
                con.close()
            except Exception as ex:
                print("Erro ao enviar a respota. Causa: ", ex.args)
        
        try:
            request = conexao.recv(1024).decode()                       # Decofica a mensagem recebida
            informacoes_request = self.__decode_requisicao(request)     # Obtém as informações da requisição
            if (str(informacoes_request[1]).upper() == self.ADM_VAZAMENTOS or str(informacoes_request[1]).upper() == self.LISTAR_CLIENTES):    # Verifica se a solicitação foi na rota de obter áreas possíveis vazamentos ou listar clientes
                executar = ObterDados('')                                       # Se sim, cria o objeto para obter os dados sem uma matrícula associada

            elif (str(informacoes_request[1]).upper() == self.LOGIN_ADM):       # Verifica se a rota requisitada foi de login de adm
                dados = json.loads(informacoes_request[-1])                     # Converte os dados recebidos da requisição em dicionário
                executar = ObterDados('')                                       # Cria o objeto para obter os dados
            else:                                                               # Senão
                dados = json.loads(informacoes_request[-1])                     # Converte os dados recebidos da requisição em dicionário                      
                executar = ObterDados(dados['matricula'])                       # Cria o objeto para obter os dados com a matrícula do usuário enviado na solicitação
            print(f"Requisição na rota: {informacoes_request[1]}")
            if (informacoes_request[0] == 'GET'):                               # Verifica se o verbo da requisição é do tipo GET
                if (str(informacoes_request[1]).upper() == self.USER_OBTER_FATURA): # Obter a Fatura do cliente
                    try:
                        resultado = executar.obter_fatura()                                                 # Obtém o resultado da operação
                        if (resultado != self.NOT_FOUND and resultado != self.INTERNAL_SERVER_ERROR and resultado != self.NOT_ACCEPTABLE):  # Se não retornar nenhum erro
                            resposta = f"HTTP/1.1 200\n\n{resultado}"                                       # Resposta da requisição
                            enviar_resposta(conexao, resposta)                                              # Envia a mensagem de resultado da ação
                        else:                                                                               # Se retornar um erro
                            resposta = f"HTTP/1.1 {resultado}\n\n"                                          # Mensagem de retorno com o erro
                            enviar_resposta(conexao, resposta)                                              # Envia a mensagem de erro
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.ADM_VAZAMENTOS):  # Possíveis vazamentos de água
                    try:
                        resultado = executar.possiveis_vazamentos()
                        if (resultado != self.NOT_FOUND and resultado != self.INTERNAL_SERVER_ERROR):    # Se não retornar nenhum erro
                            resposta = f"HTTP/1.1 200\n\n{resultado}"                                   # Retorna o json com locais de possíveis vazamentos
                            enviar_resposta(conexao, resposta)                                          # Envia a resposta
                        else:                                                                           # Se retornar um erro
                            resposta = f"HTTP/1.1 {resultado}\n\n"                                      
                            resposta(conexao, resposta)                                                 # Envia o erro
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.USER_CONSUMO_DATA_HORARIO):   # Consumo por data e hora
                    try:
                        resultado = executar.consumo_data_hora(dados)
                        if (resultado != self.NOT_FOUND and resultado != self.INTERNAL_SERVER_ERROR):   # Se não retornar nenhum erro
                            resposta = f"HTTP/1.1 200\n\n{resultado}"                                   # Retorna o json com locais de possíveis vazamentos
                            enviar_resposta(conexao, resposta)                                          # Envia a resposta
                        else:                                                                           # Se retornar um erro
                            resposta = f"HTTP/1.1 {resultado}\n\n" 
                            enviar_resposta(conexao, resposta)                                          # Envia a mensagem de resultado da ação 
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.USER_CONSUMO_TOTAL):  # Consumo total
                    try:
                        resultado = executar.consumo_total()
                        if (resultado != self.NOT_FOUND and resultado != self.INTERNAL_SERVER_ERROR):   # Se não retornar nenhum erro
                            resposta = f"HTTP/1.1 200\n\n{resultado}"                                   # Retorna o json com locais de possíveis vazamentos
                            enviar_resposta(conexao, resposta)                                          # Envia a resposta
                        else:                                                                           # Se retornar um erro
                            resposta = f"HTTP/1.1 {resultado}\n\n" 
                            enviar_resposta(conexao, resposta) 
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.USER_CONSUMO_ATUAL):  # Verifica se a solicitação é equivalente a uma rota existente
                    try:
                        resultado = executar.consumo_fatura_atual()
                        if (resultado != self.NOT_FOUND and resultado != self.INTERNAL_SERVER_ERROR):   # Se não retornar nenhum erro
                            resposta = f"HTTP/1.1 200\n\n{resultado}"                                   # Retorna o json com locais de possíveis vazamentos
                            enviar_resposta(conexao, resposta)                                          # Envia a resposta
                        else:                                                                           # Se retornar um erro
                            resposta = f"HTTP/1.1 {resultado}\n\n" 
                            enviar_resposta(conexao, resposta) 
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.LISTAR_CLIENTES): # Verifica se a solicitação é equivalente a uma rota existente
                    try:
                        resultado = executar.listar_clientes()                      # Obtém a lista de clientes
                        if (resultado != self.INTERNAL_SERVER_ERROR):               # Verifica se não ocorreu nenhum erro
                            resposta = f"HTTP/1.1 200\n\n{resultado}"               # Retorna a lista de clientes
                            enviar_resposta(conexao, resposta)                      # Envia a resposta de volta
                        else:                                                       # Se ocorreu um erro
                            resposta = f"HTTP/1.1 {resultado}\n\n"                  # Cria a mensagem de erro
                            enviar_resposta(conexao, resposta)                      # Envia a mensagem de erro
                    except:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta informando o erro
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.LOGIN_ADM):
                    try:
                        resultado = executar.login_adm(dados['login'], dados['senha'])  # Verifica se os dados correspondem ao adm
                        if (resultado != self.NOT_FOUND):                                # Se sim
                            resposta = f"HTTP/1.1 200\n\n"                              # Gera um resposta com OK
                            enviar_resposta(conexao, resposta)                          # Envia a resposta
                        else:                                                           # Senão 
                            resposta = f"HTTP/1.1 {resultado}\n\n"                      # Gera a mensagem de erro
                            enviar_resposta(conexao, resposta)                          # Envia a resposta de erro
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"         # Envia a mensagem de resposta informando o erro
                        enviar_resposta(conexao, resposta)                              # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.LOGIN_USUARIO):
                    try:
                        resultado = executar.login_cliente()        # Verifica se a matricula corresponde a algum cliente
                        if (resultado != self.NOT_FOUND):
                            resposta = f"HTTP/1.1 200\n\n"                              # Gera um resposta com OK
                            enviar_resposta(conexao, resposta)                          # Envia a resposta
                        else:                                                           # Senão 
                            resposta = f"HTTP/1.1 {resultado}\n\n"                      # Gera a mensagem de erro
                            enviar_resposta(conexao, resposta)                          # Envia a resposta de erro
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"         # Envia a mensagem de resposta informando o erro
                        enviar_resposta(conexao, resposta)                              # Envia a mensagem de resultado da ação
                else:
                    resposta = f"HTTP/1.1 {self.NOT_FOUND}\n\n" 
                    enviar_resposta(conexao, resposta) 
            elif (informacoes_request[0] == 'POST'):                    # Verifica se o verbo da requisição é do tipo POST
                if (str(informacoes_request[1]).upper() == self.ADM_DESLIGAR_CLIENTE):  # Verifica a rota solicitada é equivalente a uma rota existente
                    try:
                        resultado = executar.desligar_cliente(dados)    # Executa a ação de desligar o cliente e retorna o resultado
                        resposta = f"HTTP/1.1 {resultado}\n\n"          # mensagem de resultado da ação
                        enviar_resposta(conexao, resposta)              # Envia a mensagem de resultado da ação
                    except Exception:                                    # Caso a matricula não seja fornecida pelo cliente na requisição
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
                        enviar_resposta(conexao, resposta)              # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.SYS_RECEBER_PAGAMENTO): # Verifica a rota solicitada é equivalente a uma rota existente
                    try:
                        resultado = executar.receber_pagamento()   # Quita a fatura e retorna o resultado da ação
                        resposta = f"HTTP/1.1 {resultado}\n\n"          # mensagem de resultado da ação
                        enviar_resposta(conexao, resposta)              # Envia a mensagem de resultado da ação
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
                        enviar_resposta(conexao, resposta)              # Envia a mensagem de resultado da ação
                elif (str(informacoes_request[1]).upper() == self.ADM_GERAR_FATURA): # Gerar Fatura
                    try:
                        resultado = executar.gerar_fatura()                                             # Obtém o resultado da operação
                        resposta = f"HTTP/1.1 {resultado}\n\n"                                          # Resposta da requisição
                        enviar_resposta(conexao, resposta)                                              # # Envia a mensagem de resultado da ação
                    except Exception:
                        resposta = f"HTTP/1.1 {self.INTERNAL_SERVER_ERROR}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
                        enviar_resposta(conexao, resposta)                          # Envia a mensagem de resultado da ação
                else:
                    resposta = f"HTTP/1.1 {self.NOT_ACCEPTABLE}\n\n{informacoes_request[-1]}"   # Envia um mensagem de método não suportado e retorna o conteúdo da mensagem recebida
                    conexao.sendall(resposta.encode())
                    conexao.close()
            else:
                resposta = f"HTTP/1.1 {self.NOT_IMPLEMENTED}\n\n{informacoes_request[-1]}"   # Envia um mensagem de método não suportado e retorna o conteúdo da mensagem recebida
                conexao.sendall(resposta.encode())
                conexao.close()
        except ConnectionResetError as ex:
            print("Erro na conexão do cliente. Causa: ", ex.args)
        except KeyError:
            resposta = f"HTTP/1.1 {self.NOT_FOUND}\n\n"     # Envia a mensagem de resposta para o cliente informando o erro de matricula não encontrada
            enviar_resposta(conexao, resposta)              # Envia a mensagem de resultado da ação
        except ValueError:     
                resposta = f"HTTP/1.1 {self.NOT_ACCEPTABLE}\n\n{informacoes_request[-1]}"
                enviar_resposta(conexao, resposta)
        except Exception as ex:
            print("Erro com a conexão. Causa: ", ex.args)

    def __decode_requisicao(self, requisicao: str):
        """ Decodifica a requisição recebida """
        verbo, _, restante = requisicao.partition(' ')  # Obtém o verbo da requisição
        url, _, _ = restante.partition(' ')             # Obtém o caminho da requisição
        body = re.split(r"\n", requisicao)              # Obtém o conteúdo da requisição (body)

        if (verbo == 'GET'):
            return ['GET', url, body[-1]]       # Retorna o verbo, a rota e o conteúdo da requisição
        elif (verbo == 'POST'):
            return ['POST', url, body[-1]]      # Retorna o verbo, a rota e o conteúdo da requisição
        else:
            return ['UNKNOWN', url, body[-1]]   # Retorna o verbo desconhecido, a rota e o conteúdo da requisição

class ObterDados():
    """ Classe para obter os dados das solicitações da API """
    LIGAR =     "2"         # Constante para o comando de ligar um hidrômetro
    DESLIGAR =  "1"         # Constante para o comando de desligar um hidrômetro

    # ---------- Mensagens ----------
    NOT_FOUND =             ('404')
    NOT_ACCEPTABLE =        ('406')
    OK =                    ('200')
    INTERNAL_SERVER_ERROR = ('500')
    NOT_IMPLEMENTED =       ('501')

    
    def __init__(self, matricula: str):
        """ Construtor da classe ObterDados"""
        self.__matricula = matricula


    # ----------- Métodos Públicos ----------- #
    def desligar_cliente(self, dados: dict):
        """ Método responsável por bloquear hidrômetro de um cliente """
        return self.__bloquear_hidrometro(dados)
    
    def receber_pagamento(self):
        """ Método responsável por receber o pagamento de uma fatura de um cliente """
        return self.__conta_paga()

    def possiveis_vazamentos(self):
        """ Método responsável por obter os locais com possíveis vazamentos """
        try:
            df = pd.read_csv(f"usuarios.csv")
            return self.__obter_alertas_vazamento(df)
        except FileNotFoundError:
            return self.NOT_FOUND
    
    def consumo_data_hora(self, dados):
        """ Método responsável por retornar o consumo de um cliente em um período de tempo """
        return self.__obter_consumo_data(dados)

    def consumo_total(self):
        """ Método responsável por retornar o consumo total de um cliente """
        return self.__obter_consumo_total()

    def consumo_fatura_atual(self):
        """ Método responsável por retornar o consumo da fatura atual de um cliente """
        return self.__obter_consumo_fatura()
    
    def gerar_fatura(self):
        """ Método responsável por gerar a fatura de um cliente"""
        try:
            df_consumo = pd.read_csv(f"{self.__matricula}.csv", sep=',')                                    # Carrega o banco de dados de consumo da matricula (usuário) especificada
            df_dados = pd.read_csv("usuarios.csv", sep=",")                                                 # Carrega o banco de dados de usuários
            indice_usuario = (df_dados.index[df_dados['matricula'] == int(self.__matricula)]).tolist()[0]   # Obtém o indice que refencia o usuário no dataframe de usuários
            if (df_dados.iloc[indice_usuario]['pendencia'] == True):                                             # Verifica se o usuário tem faturas em aberto
                return self.NOT_ACCEPTABLE                                                                  # Se tiver faturas em aberto, não é possível gerar uma nova fatura ainda e retorna um erro
            else:
                fatura_json = self.__criar_fatura(df_consumo, df_dados)                                     # Gera o Json com os dados da fatura
                df_consumo.at[0, 'contaGerada'] = True                                                      # Identifica que o usuário tem uma fatura gerada
                df_consumo.at[0, 'pago'] = False                                                            # Identifica que o usuário ainda não pagou a fatura (visto que a conta acabou de criada)
                df_dados.at[indice_usuario, 'pendencia'] = True                                             # Identifica que o usuário tem uma fatura em aberto

                df_consumo.to_csv(f"{self.__matricula}.csv", sep=',', index=False)                          # Salva os dados de consumo como csv
                df_dados.to_csv("usuarios.csv", sep=",", index=False)                                       # Salva os dados dos usuários como csv
                return fatura_json                                                                          # Retorna a fatura
        except FileNotFoundError:
            return self.NOT_FOUND
        except Exception as ex:
            print("Erro ao gerar fatura. Causa: ", ex.args)
            return self.INTERNAL_SERVER_ERROR
    
    def obter_fatura(self):
        """ Método para obter a fatura do cliente """
        try:
            df_dados = pd.read_csv("usuarios.csv", sep=",")                                                 # Carrega o banco de dados de usuários
            indice_usuario = (df_dados.index[df_dados['matricula'] == int(self.__matricula)]).tolist()[0]   # Obtém o indice que refencia o usuário no dataframe de usuários
            if (df_dados.iloc[indice_usuario]['pendencia'] == True):                                        # Verifica se o usuário tem faturas em aberto
                with open(f'{self.__matricula}.json', 'r') as fatura_file:                                  # Abre o arquivo
                    fatura = json.load(fatura_file)                                                         # Carrega o arquivo em json da fatura
                fatura = json.dumps(fatura)                                                                 # Transforma o json para string em formato json
                return fatura                                                                               # Retorna a fatura em json
            else:
                return self.NOT_ACCEPTABLE                                                                  # Retorna um erro indicando que ainda não há fatura aberta para o cliente
        except FileNotFoundError:
            return self.NOT_FOUND                                                                           # Retorna um erro indicando que a matricula do cliente não foi encontrada
        except Exception as ex:
            print("Erro ao gerar fatura. Causa: ", ex.args)
            return self.INTERNAL_SERVER_ERROR
    
    def login_adm(self, login, senha):
        """ Método para efetuar o login do administrador """
        return self.__login_adm(login, senha)

    def login_cliente(self):
        """ Método para efetuar o login do cliente """
        return self.__login_cliente()
    
    def listar_clientes(self):
        """ Método para listar os clientes do sistema """
        return self.__listar_clientes()
    
    # ----------- Fim Métodos Públicos ----------- #

    # ----------- Métodos Privados ----------- #
    def __criar_fatura(self, df_consumo: DataFrame, df_dados: DataFrame):
        """ Método responsável por criar formatar os dados fatura do cliente """
        try:
            cliente = (df_dados.loc[df_dados['matricula'] == int(df_consumo.iloc[0]['matricula'])]).reset_index(drop=True)  # Obtém a linha do dataframe que pertence aos dados do usuário
            fatura = {
                'consumoFatura':    str(df_consumo.iloc[0]['consumoFatura']),   # Obtém o consumo da fatura
                'valor':            str(df_consumo.iloc[0]['valor']),           # Obtém o valor da fatura
                'dataLeitura':      str(df_consumo.iloc[0]['dataHora'])[0:10],  # Gera uma string com a data e horário exato de criação da fatura
                'consumoTotal':     str(df_consumo.iloc[0]['consumo']),         # Obtém o consumo total do cliente (valor mostrado no hidrômetro na hora da leitura) 
                'nomeUsuario':      str(df_consumo.iloc[0]['nomeUsuario']),     # Obtém o nome de usuário (cliente) da fatura
                'cpf':              str(cliente.iloc[0]['cpf']),                # obtém o cpf do cliente
                'dataFatura':       date.today().strftime("%d/%m/%Y"),          # Obtém a hora da fatura
                'dataVencimento':   (date.today() + timedelta(days=10)).strftime("%d/%m/%Y"),   # Obtém a data de vencimento da fatura
                'matricula':        self.__matricula                            # Obtém a matrícula
            }
            json_fatura = json.dumps(fatura)                                        # Cria o json com os dados da fatura
            with open(f"{self.__matricula}.json", "w") as file_fatura:              # Cria um arquivo de fatura em json para o cliente
                file_fatura.write(json_fatura)                                      # Escreve no arquivo de fatura 
            return self.OK                                                          # Retorna que a fatura foi criada
            #return json_fatura                                                     # Retorna a fatura em json
        except Exception as ex:
            print("Erro ao criar fatura. Causa: ", ex.args)
            return self.INTERNAL_SERVER_ERROR

    def __obter_alertas_vazamento(self, df_usuarios: DataFrame):
        """ Retorna um json com os endereços que há um possível vazamento """
        try:
            found = df_usuarios['possivelVazamento'] == True    # Procura os usuários que tem a flag de possíveis vazamentos
            df_vazamentos = ((df_usuarios[found]).drop(['matricula', 'nome', 'cpf', 'ativo', 'pendencia', 'possivelVazamento'], axis=1, inplace=False)).reset_index(drop=True) # Adiciona as informações dos usuários em uma dataframe
            json_vazamentos = df_vazamentos.to_json(orient='index') # Cria um JSON com os endereços com possíveis vazamentos
            return json_vazamentos  # Retorna o JSON com as regiões de possíveis vazamentos
        except Exception as ex:     # Caso ocorra algum erro
            print("Erro ao obter os alertas de vazamento. Causa: ", ex.args)
            return self.INTERNAL_SERVER_ERROR             # Retorna o erro

    def __conta_paga(self):
        """ Identifica que a conta foi paga e quita a conta do cliente no sistema"""
        try:
            df_usuarios = pd.read_csv("usuarios.csv", sep=',')
            df_dados = pd.read_csv(f"{self.__matricula}.csv")
            indice_usuario = self.__obter_indice_usuario(df_usuarios, self.__matricula)   # Obtém o indice do usuário na lista de clientes do sistema
            if (bool(df_usuarios.iloc[indice_usuario]['pendencia'])):       # Verifica se o usuário tem alguma fatura em aberto
                df_usuarios.at[indice_usuario, 'pendencia'] = False         # Retira a pendencia do cliente                                               
                os.remove(f'{self.__matricula}.json')                       # Se existir apaga o arquivo
                if (not bool(df_usuarios.iloc[indice_usuario]['ativo'])):   # Verifica se o cliente está com o fornecimento de água suspenso
                    if (self.__enviar_comando_hidrometro(df_dados.iloc[0]['host'], int(df_dados.iloc[0]['port']), self.LIGAR)):   # Ligar o hidrômetro desativado
                        df_usuarios.at[indice_usuario, 'ativo'] = True                                              # Identifica que o hidrômetro do cliente está ativo novamente
                        df_usuarios.at[indice_usuario, 'pendencia'] = False
                        df_usuarios.to_csv('usuarios.csv', sep=',', index=False)                                # Salva as modificações no arquivo
                        return self.OK
                    else:
                        return self.INTERNAL_SERVER_ERROR 
                else:
                    df_usuarios.at[indice_usuario, 'pendencia'] = False
                    df_usuarios.to_csv('usuarios.csv', sep=',', index=False)                                  # Salva as modificações no arquivo
                    return self.OK
            else:   # Se não tiver fatura em aberto
                return self.NOT_ACCEPTABLE
        except FileNotFoundError:
            return self.NOT_FOUND 

    def __bloquear_hidrometro(self, dados: dict):
        """ Método responsável por bloquear um hidrômetro """
        try:
            df_usuarios = pd.read_csv("usuarios.csv", sep=',')
            df_dados = pd.read_csv(f"{self.__matricula}.csv")
            indice_usuario = self.__obter_indice_usuario(df_usuarios, self.__matricula)   # Obtém o indice do usuário na lista de clientes do sistema 
            if (bool(df_usuarios.iloc[indice_usuario]['pendencia'])):                       # Verifica se o usuário tem uma conta em aberto
                if(self.__enviar_comando_hidrometro(df_dados.iloc[0]['host'], int(df_dados.iloc[0]['port']), self.DESLIGAR)):
                    df_usuarios.at[indice_usuario, 'ativo'] = False                         # Seta cliente como desligado
                    df_usuarios.to_csv('usuarios.csv', sep=',', index=False)                # Salva a modificação feita no arquivo
                    return self.OK
                else: return self.INTERNAL_SERVER_ERROR                                                                 
            else:
                return self.NOT_ACCEPTABLE
        except FileNotFoundError:
            return self.NOT_FOUND
        except Exception:
            return self.INTERNAL_SERVER_ERROR

    # Funcionalidade implementada, mas não disponível para uso no momento
    def __desbloquear_hidrometro(self, dados: dict):
        """ Método responsável por desbloquear um hidrômetro manualmente pelo administrador"""
        try:
            df_usuarios = pd.read_csv("usuarios.csv", sep=',')
            df_dados = pd.read_csv(f"{dados['matricula']}.csv")
            indice_usuario = self.__obter_indice_usuario(df_usuarios, dados['matricula'])   # Obtém o indice do usuário na lista de clientes do sistema 
            if (not bool(df_usuarios.iloc[indice_usuario]['ativo'])):                       # Verifica se o usuário está desativado
                if(self.__enviar_comando_hidrometro(int(df_dados.iloc[0]['host'], df_dados.iloc[0]['port']), self.LIGAR)):
                    df_usuarios.at[indice_usuario, 'ativo'] = True                         # Seta cliente como ligado
                    return self.OK
                else: return self.INTERNAL_SERVER_ERROR
            else:
                return self.NOT_ACCEPTABLE
        except FileNotFoundError:
            return self.NOT_FOUND
        except Exception:
            return self.INTERNAL_SERVER_ERROR
        
    def __obter_consumo_data(self, dados: dict):
        """ Método responsável por obter o consumo de um cliente em uma data específica"""
        try:
            df_dados = pd.read_csv(f"{self.__matricula}.csv")
            df_dados['dataHora'] = pd.to_datetime(df_dados.dataHora)
            data_inicial = datetime.strptime(dados['dataInicial'], '%Y-%m-%d-%H:%M:%S') #'%Y-%m-%d-%H:%M:%S'
            data_final = datetime.strptime(dados['dataFinal'], '%Y-%m-%d-%H:%M:%S')
            df_consumo_data = df_dados[(data_inicial <= df_dados['dataHora']) & (data_final > df_dados['dataHora'])]
            conusmo_data = float(df_consumo_data.at[df_consumo_data.index[0], 'consumo']) - float(df_consumo_data.at[df_consumo_data.index[-1], 'consumo'])  # Obtém o valor de consumo no perído especificado
            resposta = {'consumoData': conusmo_data}
            resposta = json.dumps(resposta)     # Converte a resposta para JSON
            return resposta
        except FileNotFoundError:   # Caso não encontre o cliente
            return self.NOT_FOUND
        except IndexError:          # Caso não tenha registro de consumo nas datas
            return self.NOT_FOUND
        except Exception as e:      # Caso ocorra algum outro erro
            print(e.args)
            return self.INTERNAL_SERVER_ERROR
    
    def __obter_consumo_fatura(self):
        """ Método responsável por obter o consumo da atual fatura de um cliente"""
        try:
            df_dados = pd.read_csv(f"{self.__matricula}.csv")
            consumo_fatura = float(df_dados.iloc[0]['consumoFatura'])  # Obtem o consumo da fatura atual
            resposta = {'consumoFatura': consumo_fatura}
            resposta = json.dumps(resposta)
            return resposta
        except FileNotFoundError:
            return self.NOT_FOUND
        except Exception:
            return self.INTERNAL_SERVER_ERROR
    
    def __obter_consumo_total(self):
        """ Método responsável por obter o consumo da total de um cliente"""
        try:
            df_dados = pd.read_csv(f"{self.__matricula}.csv")
            consumo_total = float(df_dados.iloc[0]['consumo'])  # Obtem o consumo da fatura atual
            resposta = {'consumoTotal': consumo_total}
            resposta = json.dumps(resposta)
            return resposta
        except FileNotFoundError:
            return self.NOT_FOUND
        except Exception:
            return self.INTERNAL_SERVER_ERROR
    
    def __login_adm(self, usuario: str, senha: str):
        """ Método responsável por validar o login do administrador """
        df = pd.read_csv("adms.csv")       # Abre a lista o arquivo de adms
        if (usuario in df['login'].values and senha in df['senha'].values):   # Verifica se existe um adm com login e senha igual ao passado
            return self.OK                                  # Retorna OK se tiver
        else:   
            return self.NOT_FOUND                           # Retorna um erro se não tiver
    
    def __login_cliente(self):
        """ Método responsável por validar o login do cliente """
        df = pd.read_csv(f"usuarios.csv")
        if (int(self.__matricula) in df['matricula'].values):            # Verifica se existe a matricula passada
            return self.OK                                  # Retorna OK se tiver
        else:
            return self.NOT_FOUND                           # Retorna um erro se não tiver
    
    def __listar_clientes(self):
        """ Método responsável por retornar os clientes cadastrados no sistema """
        try:    
            df = pd.read_csv(f"usuarios.csv")                   # Carrega os usuários do sistema
            df_lista_clientes = df.drop(['cpf','ativo','pendencia','endereco','possivelVazamento'], axis=1, inplace=False).reset_index(drop=True)   #Obtém a lista dos clientes deixando apenas o nome e a matricula
            json_lista_clientes = df_lista_clientes.to_json(orient='index')
            return json_lista_clientes
        except:
            return self.INTERNAL_SERVER_ERROR           # Retorna o erro 
    # ----------- Fim Métodos Privados ----------- #

    # ----------- Métodos Auxiliares ----------- #
    def __enviar_comando_hidrometro(self, host: str, port: int, comando: str):
        """ Método responsável por enviar comandos para um hidrômetro """
        try:
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.connect((host, port))
            tcp.sendall(f"{comando}".encode("utf-8"))               # Envia a comando para o hidrômetro
            tcp.close()
            return True
        except Exception as ex:
            print("Erro ao enviar mensagem para o hidrômetro do cliente. Causa: ", ex.args)
            return False

    def __obter_indice_usuario(self, dataframe: DataFrame, matricula: str):
        """ Método responsável por retornar o indice do usuário no DataFrame """
        return (dataframe.index[dataframe['matricula'] == int(matricula)]).tolist()[0]
    # ----------- Fim Métodos Auxiliares ----------- #