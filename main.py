import pandas as pd
import pyodbc
from faker import Faker
import random

server = 'DESKTOP-I6IFBTN\SQLEXPRESS'  # ou '127.0.0.1'
database = 'A3copy'  # Substitua pelo nome do seu banco de dados
driver = '{SQL Server}'  # Pode variar conforme a instalação do driver

connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes'

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
 
 # Inicializa o Faker para dados brasileiros
fake = Faker('pt_BR')

# Carrega o dataset CSV
df = pd.read_csv('tudo.csv', delimiter=',')

# Seleciona e renomeia colunas
df = df[['created_at', 'name', 'product', 'price_x', 'quantity']]
df.columns = ['data', 'nome', 'produto', 'preco', 'quantidade']

# Converte a coluna de data e remove fuso horário (timezone-naive)
df['data'] = pd.to_datetime(df['data']).dt.tz_localize(None)
df['preco'] = df['preco'] / 100
df['quantidade'] = df['quantidade'].astype(int)

# Define um intervalo de datas recentes (por exemplo, entre 2015 e 2023)
start_date = pd.to_datetime('2019-01-01')
end_date = pd.to_datetime('2023-12-31')

# Função para gerar uma data aleatória no intervalo
def random_recent_date():
    return start_date + (end_date - start_date) * random.random()

# Substitui as datas antigas (por exemplo, antes de 2000) com datas aleatórias recentes
df['data'] = df['data'].apply(lambda x: random_recent_date() if x < pd.Timestamp('2000-01-01') else x)

# Ajusta o formato da data para 'dd/mm/yyyy'
df['data'] = df['data'].dt.strftime('%Y-%m-%d')

# Substitui os nomes da coluna 'nome' com novos nomes gerados pelo Faker
for i in range(len(df)):
    df.at[i, 'nome'] = fake.name()

# Lista de produtos específicos
produtos_especificos = ["Cópia", "Encadernação", "Impressão", "Digitalização", "Plastificação", "Gravação de CD/DVD"]

# Substitui os nomes dos produtos por valores aleatórios da lista de produtos específicos
df['produto'] = df['produto'].apply(lambda x: random.choice(produtos_especificos))

for index, row in df.iterrows():
        insert_query = """
        INSERT INTO base_dados (data, nome, produto, preco, quantidade)
        VALUES (?, ?, ?, ?, ?)
        """
        valores = (row['data'], row['nome'], row['produto'], row['preco'], row['quantidade'])
        cursor.execute(insert_query, valores) 
        
conn.commit()