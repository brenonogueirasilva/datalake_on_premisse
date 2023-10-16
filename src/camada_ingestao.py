from minio import Minio
from minio.error import S3Error
import psycopg2
from io import StringIO
import os
import requests
import json
import pymongo
from bson.json_util import dumps



minio_client = Minio(
    "minio:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

def insert_minio(nome_arquivo: str, bucket: str, caminho: str):
    '''
    Insere um arquivo no MinIO (serviço de armazenamento de objetos) em um bucket especificado.

    Args:
        nome_arquivo (str): O nome do arquivo que será inserido no MinIO.
        bucket (str): O nome do bucket onde o arquivo será armazenado.
        caminho (str): O caminho local para o arquivo que será enviado ao MinIO.
    '''
    try:
        minio_client.fput_object(
            bucket,
            nome_arquivo,  
            caminho  
        )
        print("Upload realizado com sucesso!")
    except S3Error as err:
        print(err)

def list_minio(nome_bucket: str):
    '''
    Lista todos os objetos em um bucket MinIO especificado.

    Args:
        nome_bucket (str): O nome do bucket do qual deseja listar os objetos.
    '''
    try:
        objects = minio_client.list_objects(nome_bucket, recursive=True)
        for obj in objects:
            print(obj.object_name)
    except S3Error as err:
        print(err)


#1. Arquivo Local
def insert_arquivo_local(caminho: str):
    '''
    Insere um arquivo local no MinIO em um bucket chamado 'bronze'.
    Args:
        caminho (str): O caminho local completo para o arquivo que será enviado ao MinIO.
    '''
    nome_arquivo = caminho.split('/')[-1]
    insert_minio(nome_arquivo, 'bronze', caminho)
    
insert_arquivo_local("./files/annual-enterprise-2021.csv")

#2. Banco SQL de produção
def select_to_csv_postgres(sql: str):
    '''
    Executa uma consulta SQL em um banco de dados PostgreSQL e retorna o resultado como uma string CSV.
    Args:
        sql (str): A consulta SQL a ser executada no banco de dados.
    '''
    postgres_config = {
        'host': 'postgres',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres',
    }
    try:

        conn = psycopg2.connect(**postgres_config)
        cursor = conn.cursor()
        output = StringIO()
        comando = f"COPY ({sql}) TO STDOUT WITH CSV HEADER"
        cursor.copy_expert(comando, output)
        csv = output.getvalue()
        return csv
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro ao conectar ao PostgreSQL:", error)
    finally:
        if conn is not None:
            conn.close()

def insert_csv_to_tabela_relacional(sql, nome_arquivo):
    '''
    Executa uma consulta SQL em um banco de dados PostgreSQL, extrai o resultado no formato CSV
    e salva-o em um arquivo local antes de enviá-lo para o sistema Minio.

    Args:
        sql (str): A consulta SQL a ser executada no banco de dados.
        nome_arquivo (str): O nome do arquivo que será criado e armazenado no sistema Minio.
    '''
    arquivo_csv = select_to_csv_postgres(sql)
    with open('./files/' + nome_arquivo, 'w', newline='') as csv_arquivo:
            csv_arquivo.write(arquivo_csv)
    insert_minio(nome_arquivo, 'bronze', './files/' + nome_arquivo)
    
insert_csv_to_tabela_relacional("select * from research", 'research.csv')

#3. Salvando Arquivos HTML para realizar webscrapping posteriormente
def download_html(url: str, nome_arquivo: str):
    '''
    Realiza uma requisição HTTP GET para a URL fornecida, faz o download do conteúdo HTML
    da página e salva em um arquivo local no diretório './files/'.

    Args:
        url (str): A URL da página da qual se deseja baixar o conteúdo HTML.
        nome_arquivo (str): O nome do arquivo de destino onde o HTML será salvo.
    '''
    try:
        response = requests.get(url)
        if response.status_code == 200:
            html_content = response.text
            arquivo_destino = './files/' + nome_arquivo 
            with open(arquivo_destino, 'w', encoding='utf-8') as arquivo:
                arquivo.write(html_content)
            print("Código fonte HTML da página foi salvo em um arquivo com sucesso!")
        else:
            print(f"Falha na requisição: Código de status {response.status_code}")
    except requests.RequestException as e:
        print("Erro na requisição HTTP:", e)

def insert_html(url: str, nome_arquivo: str):
    '''
    Faz o download do código fonte HTML de uma página da web e, em seguida, insere-o no sistema Minio.

    Args:
        url (str): A URL da página da qual se deseja baixar o conteúdo HTML.
        nome_arquivo (str): O nome do arquivo que será criado e armazenado no sistema Minio.
    '''
    download_html(url, nome_arquivo)
    insert_minio(nome_arquivo, 'bronze', './files/'+ nome_arquivo)
    
insert_html("https://lista.mercadolivre.com.br/3ds#D[A:3ds]", 'html_mercado_livre.html' )

#4. Json de uma API
def download_json_api(url: str, nome_arquivo: str):
    '''
    Realiza uma requisição HTTP GET para a URL fornecida, faz o download do conteúdo JSON
    da resposta e salva em um arquivo local no diretório './files/'.

    Args:
        url (str): A URL da qual se deseja baixar o conteúdo JSON.
        nome_arquivo (str): O nome do arquivo de destino onde o JSON será salvo.
    '''
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            arquivo_destino = './files/' + nome_arquivo

            with open(arquivo_destino, 'w', encoding='utf-8') as arquivo:
                json.dump(json_data, arquivo, indent=4)

            print("JSON foi salvo em um arquivo na sua máquina com sucesso!")
        else:
            print(f"Falha na requisição: Código de status {response.status_code}")

    except requests.RequestException as e:
        print("Erro na requisição HTTP:", e)

def insert_json_api(url: str, nome_arquivo: str):
    '''
    Faz o download de um arquivo JSON de uma URL e, em seguida, insere-o no sistema Minio.

    Args:
        url (str): A URL da qual se deseja baixar o conteúdo JSON.
        nome_arquivo (str): O nome do arquivo que será criado e armazenado no sistema Minio.
    '''
    download_json_api(url, nome_arquivo)
    insert_minio('api_teste.json' , 'bronze' , './files/' + nome_arquivo)
    
insert_json_api("https://api.coindesk.com/v1/bpi/currentprice.json", "api.json" )

#5. Banco NoSQL
def conect_mongodb(collection: str, nome_arquivo: str):
    '''
    Conecta-se a um banco de dados MongoDB, busca dados na coleção especificada
    e salva os dados em um arquivo JSON local.

    Args:
        collection (str): O nome da coleção do banco de dados MongoDB a ser consultada.
        nome_arquivo (str): O nome do arquivo onde os dados JSON serão salvos localmente.
    '''
    mongo_config = {
        'host': 'mongodb',
        'port': 27017,  
        'database': 'dados',
        'collection': collection
    }
    url = "mongodb://root:root@mongodb:27017/"
    json_file_path = './files/' + nome_arquivo
    try:
        client = pymongo.MongoClient(url)
        db = client[mongo_config['database']]
        collection = db[mongo_config['collection']]
        cursor = collection.find({})
        with open(json_file_path, 'w') as json_file:
           json.dump(json.loads(dumps(cursor)), json_file)
        print("Dados da coleção foram salvos como JSON localmente com sucesso!")

    except Exception as e:
        print("Erro ao buscar e salvar os dados como JSON:", e)
        
def insert_mongodb(collection: str, nome_arquivo: str):
    conect_mongodb(collection, nome_arquivo)
    insert_minio(nome_arquivo, 'bronze', './files/' + nome_arquivo)
    
insert_mongodb('dados', 'cores.json')