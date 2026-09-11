import pandas as pd
import random
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, asin
from time import sleep

# ///////////////////////////////////////// Variáveis ///////////////////////////////////////////////////////// #

velocidade_coleta = 0.001 # quantas vezes mais rápido ele vai (obter massas maiores mais rápido)

quantidade_coleta = 20    # quantos patinetes estarão se movendo

intervalo_coleta  =  30   # de quanto em quanto tempo ele coleta, em segundos(muda a distância entre os pontos)

data_inicio = datetime(2026, 1, 1, 12, 30)

limit = 20000 # total registros POR PATINETE

# ///////////////////////////////////////////////////////////////////////////////////////////////////////////// #

# ///////////////////////////////////////// Configurações ///////////////////////////////////////////////////// #

with open('output.csv', 'w') as f:
    f.write('id_patinete, lat, lon, dh\n')
output_file = open('output.csv', 'a+')

df = pd.read_csv('estacoes.csv')

arr = []

for i in range(quantidade_coleta):
    temp1 = df.sample(1)
    temp2 = df.sample(1)
    arr.append({'id': i,'atual': {'lat': temp1['lat'].item(), 'lon':temp1['lon'].item(), 'dh': data_inicio }, 'destino': {'lat': temp2['lat'].item(), 'lon': temp2['lon'].item()}})

# ///////////////////////////////////////////////////////////////////////////////////////////////////////////// #

def get_distance(coord1, coord2):
    R = 6371.0088

    lat1 = coord1['lat']
    lon1 = coord1['lon']
    lat2 = coord2['lat']
    lon2 = coord2['lon']

    lat_dist = radians(lat2 - lat1)
    lon_dist = radians(lon2 - lon1)

    a = sin(lat_dist / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(lon_dist / 2)**2
    c = 2 * asin(sqrt(a))

    # em KM
    return R * c

def get_closest_spots(row):
    i = 0
    closest1 = (0, 999999999, 'lat', 'lon')
    closest2 = (0, 999999999, 'lat', 'lon')


    coord1 = (row['lat'], row['lon'])

    while i < len(df2):
        coord2 = (df2['lat'][i], df2['lon'][i])
        dist = get_distance(coord1, coord2)
        if dist < closest1[1]:
            closest2 = closest1 
            closest1 = (df2['id'][i], dist, df2['lat'][i], df2['lon'][i])
        elif dist < closest2[1]:
            closest2 = (df2['id'][i], dist, df2['lat'][i], df2['lon'][i]) 
        i+=1
    return (closest1[0], closest1[2], closest1[3], closest2[0], closest2[2], closest2[3])

def enviar_pos_atual(item):
    payload = {
        'id': item['id'],
        'lat': item['atual']['lat'],
        'lon': item['atual']['lon'],
        'dh':  item['atual']['dh']
    }
    output_file.write(f'{payload['id']}, {payload['lat']}, {payload['lon']}, {payload['dh']} \n')

def criar_destino():
    temp1 = df.sample(1)
    return {'lat': temp1['lat'].item(), 'lon': temp1['lon'].item()}

def aproximar(item):

    if (item['atual']['lat'] - item['destino']['lat'] > 0):
        item['atual']['lat'] -= (random.randint(30, 160) / 100_000)
    else:
        item['atual']['lat'] += (random.randint(30, 160) / 100_000)

    if (item['atual']['lon'] - item['destino']['lon'] > 0):
        item['atual']['lon'] -= (random.randint(30, 160) / 100_000)
    else:
        item['atual']['lon'] += (random.randint(30, 160) / 100_000)

    item['atual']['dh'] = item['atual']['dh'] + timedelta(seconds=intervalo_coleta)
    
    return {'lat': item['atual']['lat'], 'lon': item['atual']['lon'], 'dh': item['atual']['dh']}

j = 0

while True:
    j = j + 1

    if j > limit:
        break
    # Por passagem, andar (ou escolher novo destino) para patinete

    # Enviar dados ANTES de mover!!

    # Mover 

    for index, item in enumerate(arr):

        enviar_pos_atual(item)

        dist = get_distance(item['atual'], item['destino'])
        
        # chegou no destino -> escolher novo destino (chance aleatoria para simular pessoas trocando / patinete esperando)
        if dist == 0 and random.random() > 0.9:
            arr[index]['destino'] = criar_destino()

        # distância menor que 200 metros
        if get_distance(item['atual'], item['destino']) < 0.2:
            new_dh = arr[index]['atual']['dh'] + timedelta(seconds=intervalo_coleta)
            arr[index]['atual'] = arr[index]['destino']
            arr[index]['atual']['dh'] = new_dh

        # andar
        else:
            arr[index]['atual'] = aproximar(item)

    sleep(velocidade_coleta)
