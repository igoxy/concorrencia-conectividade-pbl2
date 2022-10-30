# -*- coding: utf-8 -*-

# como rodar a aplicação com debug: python -m flask --app start --debug run

from datetime import datetime
import json
from flask import request, redirect, render_template, url_for, Flask
import requests

endereco_servidor = input("Informe o endereço do servidor da API: ")

app = Flask(__name__)


# ------ Página inicial ---------
@app.route("/")     # Página inicial
def index():
    return render_template('index.html')
# ............. FIM ...............


# ------ Páginas de login ---------
@app.route('/adm_login', methods=['GET', 'POST'])   # Login adm
def adm_login():
    """ Função para tratar a rota de login do administrador """
    if (request.method == 'POST'):
        if request.form.get('enviar') == 'Entrar':  # Verifica se o botão foi acionado
            login = request.form.get('login')   # Obtém o login inserido
            senha = request.form.get('senha')   # Obtém a senha inserida
            dados = {'login': login, 'senha': senha}    # Cria o dicionário de dados
            resposta = requests.get(f'http://{endereco_servidor}:5050/LOGIN_ADM', json=dados)   # Cria o dicionário de dados
            if (resposta.status_code == 200):   # Cria o dicionário de dados
                return redirect(url_for('adm_home'))    # Se foi OK redireciona para a página inicial do adm
            else:                               # Se ocorrer um erro
                return render_template('adm_login.html', mensagem_erro = "Usuário ou senha incorreta. Tente novamente.")    # Mostra a mensagem de erro
    else:
        return render_template('adm_login.html')

@app.route('/cliente_login', methods=['GET', 'POST'])       # Login cliente
def cliente_login():
    """ Função para tratar a rota de login do cliente """
    if (request.method == 'POST'):
        if request.form.get('enviar') == 'Entrar':      # Verifica se o botão foi acionado
            matricula = str(request.form.get('matricula')).zfill(3)   # Obtém a matricula inserida
            dados = {'matricula': matricula}            # Cria o dicionário de dados
            resposta = requests.get(f'http://{endereco_servidor}:5050/LOGIN_USUARIO', json=dados)   # Cria o dicionário de dados
            if (resposta.status_code == 200):   # Cria o dicionário de dados
                return redirect(url_for('cliente_home', matricula=matricula))   # Se foi OK redireciona para a página inicial do cliente
            else:       # Se ocorrer um erro
                return render_template('cliente_login.html', mensagem_erro = "Matrícula inválida. Tente novamente!")     # Mostra a mensagem de erro
    else:
        return render_template('cliente_login.html')
# ............. FIM ...............


# ------ Página pagamento ----------
@app.route('/pagamento', methods=['GET', 'POST'])           # Página de Pagamento
def pagamento():
    """ Função para tratar a rota pagamento de faturas """
    if (request.method == 'POST'):                          # Verifica se foi um método post
        if (request.form.get('Pagar')) == 'Pagar':          # Verifica se o botão foi pressionado
            matricula = str(request.form.get('matricula')).zfill(3)  # Obtém a matricula fornecida
            dados = {'matricula': matricula}                # Gera o dicionario com o a matrícula
            resposta = requests.post(f'http://{endereco_servidor}:5050/PAGAMENTO/RECEBER', json=dados)
            if (resposta.status_code == 200):                   # Verifica se o código de retorno da API foi OK
                return render_template('pagamento.html', mensagem='Pagamento feito com sucesso!')
            elif (resposta.status_code == 406):                 # Verifica se o código de retorno da API foi NOT_ACCEPTABLE
                return render_template('pagamento.html', mensagem='Erro! O Cliente não tem fatura em aberto.')
            elif (resposta.status_code == 404):                 # Verifica se o código de retorno da API foi NOT_FOUND
                return render_template('pagamento.html', mensagem='Erro! Cliente não encontrado.')
            else:                                               # Verifica se o código de retorno da API foi qualquer outro
                return render_template('pagamento.html', mensagem='Erro! Não foi possível efetuar a operação.')
        else:
            return render_template('pagamento.html', mensagem='')    # Caso não seja um POST
    else: 
        return render_template('pagamento.html', mensagem='')    # Caso não seja um POST
# ............. FIM ...............


# ------ Páginas dos usuários ------
@app.route('/cliente_home/<matricula>')                                     # Página inicial
def cliente_home(matricula):
    """ Função para tratar a página inicial do cliente """    
    return render_template('cliente_home.html', matricula=matricula)

@app.route('/cliente_home/<matricula>/consumo_fatura', methods=['GET'])     # Consumo da fatura atual
def consumo_fatura(matricula):
    """ Função para tratar a rota de visualização de consumo da fatura do cliente """
    dados = {'matricula':str(matricula).zfill(3)}
    resposta = requests.get(f'http://{endereco_servidor}:5050/CLIENTE/CONSUMO/FATURA_ATUAL', json=dados)  # Obtém a resposta da API
    if (resposta.status_code == 200):                   # Verifica se a resposta foi OK
        dados = json.loads(resposta.content.decode())
        return render_template('cliente_consumo_fatura.html', matricula=matricula, consumo_fatura_atual=dados["consumoFatura"], mostrar_consumo=True)     # Retorna a página com o consumo para o usuário
    else:       # Caso a resposta da API não seja OK
        return render_template('cliente_consumo_fatura.html', matricula=matricula, consumo_fatura_atual='', mostrar_consumo=False)  # Retorna um erro para o usuário

@app.route('/cliente_home/<matricula>/consumo_periodos', methods=['POST', 'GET'])   # Consumo em determinados períodos
def consumo_periodos(matricula):
    """ Função para tratar a rota de visualização de consumo cliente em um período """
    data_atual = datetime.today().strftime('%Y-%m-%d %H:%M')
    if request.method == 'POST':        # Verifica se o método solicitado foi o post
        if (request.form.get('Buscar') == 'Buscar'):    # Verifica se o botão de buscar foi acionado
            data_inicial = str(request.form.get('data_inicial')).replace('T', '-')
            data_final = str(request.form.get('data_final')).replace('T', '-')
            dados = {'matricula': str(matricula).zfill(3), 'dataInicial': data_inicial, 'dataFinal': data_final}
            resposta = requests.get(f'http://{endereco_servidor}:5050/CLIENTE/CONSUMO/DATA_HORARIO', json=dados)
            if (resposta.status_code == 200):       # Se obteve o consumo com sucesso
                dados = resposta.json()             # Obtém o consumo da resposta da api
                consumo = dados['consumoData']      # Adiciona o consumo a variável
                return render_template('cliente_consumo_periodos.html', data_inicial=data_inicial, data_final=data_final, matricula=matricula, consumo_periodo=consumo, mostrar_consumo=True, data_maxima=data_atual, mensagem='')  # Retorna o consumo para o usuário
            else:
                mensagem = 'Erro ao obter o consumo no período indicado. Tente novamente!' # Mensagem de erro caso não consiga obter o consumo
                return render_template('cliente_consumo_periodos.html', data_inicial=data_inicial, data_final=data_final, matricula=matricula, consumo_periodo='', mostrar_consumo=False, data_maxima=data_atual, mensagem=mensagem)    # Retorna a mensagem de erro para o usuário
    else:
        return render_template('cliente_consumo_periodos.html', data_inicial='', data_final='', matricula=matricula, consumo_periodo='', mostrar_consumo=False, data_maxima=data_atual, mensagem='')    # Retorna a página sem mensagem para o usuário
    return render_template('cliente_consumo_periodos.html', data_inicial='', data_final='', matricula=matricula, consumo_periodo='', mostrar_consumo=False,  data_maxima=data_atual, mensagem='')       # Retorna a página sem mensagem para o usuário

@app.route('/cliente_home/<matricula>/consumo_total', methods=['GET'])      # Consumo total do cliente
def consumo_total(matricula):
    """ Função para tratar a rota de visualização de consumo total do cliente """
    dados = {'matricula': matricula}
    resposta = requests.get(f'http://{endereco_servidor}:5050/CLIENTE/CONSUMO/TOTAL', json=dados)
    total = resposta.json()
    if (resposta.status_code == 200): #.status_code
        return render_template('cliente_consumo_total.html', mostrar_consumo=True, matricula=matricula, consumo_total=total['consumoTotal'])
    else:
        return render_template('cliente_consumo_total.html', mostrar_consumo=False, matricula=matricula, consumo_total='')

@app.route('/cliente_home/<matricula>/fatura', methods=['GET'])             # Consumo da fatura atual
def fatura(matricula):
    """ Função para tratar a rota de obtenção de fatura do cliente """
    dados = {'matricula': matricula}
    resposta = requests.get(f'http://{endereco_servidor}:5050/CLIENTE/OBTER_FATURA', json=dados)
    if (resposta.status_code == 200):
        dados_resposta = resposta.json()
        return render_template('fatura.html', nome=dados_resposta['nomeUsuario'], cpf=dados_resposta['cpf'], data_leitura=dados_resposta['dataLeitura'], data_vencimento=dados_resposta['dataVencimento'], consumo_fatura=dados_resposta['consumoFatura'], consumo_total=dados_resposta['consumoTotal'], matricula=dados_resposta['matricula'], valor=dados_resposta['valor'], data_fatura=dados_resposta['dataFatura']) # Cria a página html para fatura
    elif resposta.status_code == 406:       # Erro de fatura não aberta
        return render_template('erro_fatura.html', erro=True)
    else:                                   # Outros erros
        return render_template('erro_fatura.html', erro=False)
# ............. FIM ...............


# ---------- Páginas do adm ----------
@app.route('/adm_home')                                     # Página inicial do administrador
def adm_home():
    """ Função para tratar a página inicial do administrador """
    return render_template('adm_home.html')

@app.route('/adm_home/clientes', methods=['GET', 'POST'])   # Página de listar os clientes
def clientes():
    """ Função para tratar a página de listagem dos clientes para o administrador """
    resposta_lista = requests.get(f'http://{endereco_servidor}:5050/LISTAR_CLIENTES')       # Faz a requisição da lista de usuários
    dados = resposta_lista.json()               # Carrega a lista de usuários
    if (request.method == 'POST'):              # Se for o método POST
        matricula_selecionada = str(request.form.get('cliente')).zfill(3)     # Obtém o cliente selecionado
        enviar = {'matricula': matricula_selecionada}           # Cria um dicionário com a matricula do cliente
        if (request.form.get('botao') == 'Gerar fatura do cliente'):    # Verifica se o botão pressionado foi de gerar fatura
            resposta_fatura = requests.post(f'http://{endereco_servidor}:5050/ADMINISTRADOR/GERAR_FATURA', json=enviar) # Envia a requisição para API
            if (resposta_fatura.status_code == 200):            # Verifica se a requisição da API foi OK
                return render_template('lista_clientes.html', mensagem='Fatura Gerada!', erro=False, clientes=dados) # Retorna a página com uma mensagem de sucesso
            elif (resposta_fatura.status_code == 406):          # Se já tiver fatura em aberto
                return render_template('lista_clientes.html', mensagem='Não foi possível gerar fatura! Cliente já apresenta uma fatura em aberto.', erro=False, clientes=dados)    # Retorna a página com uma mensagem de falha
            else:                                               # Senão for OK
                return render_template('lista_clientes.html', mensagem='Não possível gerar a fatura do cliente.', erro=False, clientes=dados)    # Retorna a página com uma mensagem de falha
        elif (request.form.get('botao') == 'Bloquear cliente'):     # Verifica se o botão pressionado foi de bloquear cliente
            resposta_bloquear = requests.post(f'http://{endereco_servidor}:5050/ADMINISTRADOR/DESLIGAR_CLIENTE', json=enviar)
            if (resposta_bloquear.status_code == 200):            # Verifica se a resposta da requisição da API foi OK
                return render_template('lista_clientes.html', mensagem='Cliente bloqueado!', erro=False, clientes=dados) # Retorna a página com uma mensagem de sucesso
            elif (resposta_bloquear.status_code == 406):
                return render_template('lista_clientes.html', mensagem='Não é possível bloquear o cliente pois não apresenta faturas em aberto!', erro=False, clientes=dados) # Retorna a página com uma mensagem de sucesso
            else:                                               # Senão for OK
                return render_template('lista_clientes.html', mensagem='Não é possível bloquear o cliente pois o hidrômetro não está conectado ao servidor.', erro=False, clientes=dados)    # Retorna a página com uma mensagem de falha
        else:
            return render_template('lista_clientes.html', mensagem='', erro=False, clientes=dados)    # Retorna a página sem mensagem
    else:   # Se for o método GET 
        if resposta_lista.status_code == 200:                                           # Se obter a lista de clientes
            return render_template('lista_clientes.html', clientes=dados, erro=False)   # Mostra os clientes
        else:                                                                           # Senão
            return render_template('lista_clientes.html', clientes={}, erro=True)       # Exibe uma mensagem de erro

@app.route('/adm_home/vazamentos', methods=['GET'])         # Página de listar os endereços com possíveis vazamentos
def vazamentos():
    """ Função para tratar a página de endereços das possíveis regiões com vazamentos """
    resposta = requests.get(f'http://{endereco_servidor}:5050/ADMINISTRADOR/VAZAMENTOS')
    if ( resposta.status_code == 200):   
        if (len(resposta.json()) > 0):      # Verifica se há endereços com possível vazamento
            dados = resposta.json()
            return render_template('adm_vazamentos.html', erro=False, enderecos=dados)  # Exibe os locais com possíveis vazamentos
        else:                               # Senão tiver
            sem_vazamentos = {'0': {'endereco': 'Não há endereços com possível vazamento.'}}     # Exibe a informação que não há endereços com possível vazamento
            return render_template('adm_vazamentos.html', erro=False, enderecos=sem_vazamentos)
    else:
        return render_template('adm_vazamentos.html', erro=True, enderecos='')
# .............. FIM ...............

# Roda app
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)