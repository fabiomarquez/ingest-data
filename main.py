import requests
import pandas as pd
import json
import logging
import time

URL = 'https://data.wa.gov/resource/n8q6-4twj.json'

# Objetivos
# 1. Capturar dados [OK]
# 2. Usar parâmetros para capturar todos os dados
# 3. Inserir os dados em suas respectivas tabelas
# 4. Capturar logs e tempos de carga

keep_quering = True
offset = 0
final_df = pd.DataFrame()
start_time = time.time()

while keep_quering:
   
    url_query = URL + f'?$limit=1000&$offset={offset}'
    
    request = requests.get(url_query)

    request_json = request.json()

    df = pd.json_normalize(request_json)
    
    final_df = pd.concat([final_df, df])
    
    offset += 1000
    
    if len(df) < 1000:
        keep_quering = False
        end_time = time.time()
    
    print(f'Offset atual: {int(offset/1000)}')

print(f'Final do processo! {end_time - start_time}')
