import pandas as pd
from pymongo import MongoClient


#RODAR APENAS UMA VEZ, DO CONTRARIO IRA DUPLICAR OS DADOS
#NÃO CRIEI UM MODULO ESPECÍFICO APENAS PARA INSERIR OS DADOS PARA NÃO FUGIR MUITO DO QUE FOI PEDIDO NO CASE

#Dataframe com as informações dos carros
df_info_car = pd.DataFrame({
    'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
    'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']
})

#Dataframe com as informações das montadoras
df_info_manufacturer = pd.DataFrame({
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
    'Pais': ['EUA','Alemanha','França','EUA','Japão']
})

#Cria a conexão com o mongodb, no banco que foi criado previamente
client = MongoClient('localhost', 27017)  
conn = client['case_test'] 


#Insere os dados nas colections com as informações dos dataframes
df_info_car_dict = df_info_car.to_dict(orient='records')
df_info_manufacturer_dict = df_info_manufacturer.to_dict(orient='records')

conn['Carros'].insert_many(df_info_car_dict)
conn['Montadoras'].insert_many(df_info_manufacturer_dict)

# Fechando a conexão com o Mongodb
client.close()
