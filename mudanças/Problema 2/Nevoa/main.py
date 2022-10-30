# -*- coding: utf-8 -*-
from Server import Server
import pandas as pd
import numpy as np

def carregar_consumo():
    """ Carrega último valor de consumo de cada cliente """
    try:
        df = pd.read_csv('consumo-clientes.csv', sep=',')       # DataFrame para aramazenar o último consumo dos clientes
    except FileNotFoundError:
        data = {'matricula': ['001', '002', '003', '004'],      # Matrícula dos clientes da nevoa
                'consumo':  [np.NaN, np.NaN, np.NaN, np.NaN],    # Valor de consumo inicial - nenhum tem nada
                'vazao':    [np.NaN, np.NaN, np.NaN, np.NaN]
        }
        df = pd.DataFrame(data)                                 # DataFrame para aramazenar o último consumo dos clientes
        df.to_csv('consumo-clientes.csv', sep=',', index=False)
    
    return df
'''   
data = {'matricula': ['001', '002', '003', '004'],      # Matrícula dos clientes da nevoa
        'consumo':  [np.NaN, np.NaN, np.NaN, np.NaN],         # Valor de consumo inicial - nenhum tem nada
        'vazao':    [np.NaN, np.NaN, np.NaN, np.NaN]
        }
'''
df = carregar_consumo()
nome_nevoa = input("Insira a identificação da nevoa: ")
#df = pd.DataFrame(data)                                 # DataFrame para aramazenar o último consumo dos clientes
print(df.loc[df['consumo'] == 3].to_dict('records'))
print(df)

#ult_consumo_clientes = carregar_consumo()
#servidor = Server(consumo_clientes=ult_consumo_clientes, identificacao_nevoa=nome_nevoa)

input("tecle enter para sair\n")