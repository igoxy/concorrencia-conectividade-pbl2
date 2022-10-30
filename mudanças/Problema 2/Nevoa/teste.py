import json
import numpy as np
import pandas as pd
import math

data = {'matricula': ['001', '002', '003', '004'],      # Matrícula dos clientes da nevoa
        'consumo':  [np.NaN, np.NaN, np.NaN, np.NaN],    # Valor de consumo inicial - nenhum tem nada
        'vazao':    [np.NaN, np.NaN, np.NaN, np.NaN]
        }
df = pd.DataFrame(data)                                 # DataFrame para aramazenar o último consumo dos clientes

print(df)
print((df['consumo'].mean()))

print(math.isnan(df['consumo'].mean()))
if df['consumo'].mean() == np.NaN:
    print('entrou')

'''
i = 6
c = 2.0

print(i/c)
dados = {'identificacao': 'dsa', 'media': 1}
dados_json = json.dumps(dados)

print(type(dados_json))

lista = [1,2,3,4,5,6,7,8,7,np.NAN, -1, -2, 0]
print(lista)
result = filter(lambda val: val >=  0, lista) 
print(list(result))
if (np.NaN not in lista):
    print("não tem")
d = {'teste': 12, 'teste2': 32}
print(d)
d['teste'] = 43
print(d)
'''