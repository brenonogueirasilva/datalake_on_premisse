from minio import Minio
from minio.error import S3Error

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import pandas as pd


minio_client = Minio(
    "minio:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

def list_buckets_minio():
    '''
    Lista todos os buckets disponíveis no sistema Minio e imprime seus nomes.

    Raises:
        S3Error: Se ocorrer um erro ao listar os buckets.
    '''
    try:
        buckets = minio_client.list_buckets()
        print(buckets)
    except S3Error as err:
        print(err)
    
def list_objects_buckets_minio(nome_bucket: str):
    '''
    Lista todos os objetos em um bucket específico no sistema Minio, de forma recursiva,
    e imprime seus nomes.

    Args:
        nome_bucket (str): O nome do bucket que contém os objetos a serem listados.
    '''
    try:
        objects = minio_client.list_objects(nome_bucket, recursive=True)
        for obj in objects:
            print(obj.object_name)
    except S3Error as err:
        print(err)
        
def get_objects_minio(bucket: str, object: str):
    '''
    Obtém um objeto específico de um bucket no sistema Minio.

    Args:
        bucket (str): O nome do bucket onde o objeto está armazenado.
        object (str): O nome do objeto a ser obtido.
    '''
    try:
        response = minio_client.get_object( bucket, object)
        return response
    finally:
        response.close()
        response.release_conn()
        
def insert_minio(nome_arquivo: str, bucket: str, caminho: str):
    '''
    Faz o upload de um arquivo local para um bucket no sistema Minio.

    Args:
        nome_arquivo (str): O nome do arquivo a ser enviado para o sistema Minio.
        bucket (str): O nome do bucket onde o arquivo será armazenado.
        caminho (str): O caminho local do arquivo a ser enviado.
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
        
def download_minio(nome_arquivo: str, bucket: str, caminho: str):
    '''
    Faz o download de um arquivo de um bucket no sistema Minio para o sistema local.

    Args:
        nome_arquivo (str): O nome do arquivo a ser baixado do sistema Minio.
        bucket (str): O nome do bucket onde o arquivo está armazenado.
        caminho (str): O caminho local onde o arquivo será salvo após o download.
    '''
    try:
        minio_client.fget_object(
            bucket,
            nome_arquivo,  
            caminho  
        )
        print("Upload realizado com sucesso!")
    except S3Error as err:
        print(err)

# the Spark session should be instantiated as follows
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "./jars/aws-java-sdk-bundle-1.11.1026.jar, ./jars/hadoop-aws-3.3.2.jar") \
    .getOrCreate()

sc = spark.sparkContext

spark.sparkContext.setLogLevel("WARN")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def pyspark_read_csv(bucket: str, arquivo: str):
    '''
    Lê um arquivo CSV de um bucket no sistema Minio usando o Apache Spark.

    Args:
        bucket (str): O nome do bucket no sistema Minio.
        arquivo (str): O nome do arquivo CSV a ser lido.
    '''
    url = f"s3a://{bucket}/{arquivo}"
    df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(url)
    return df
    
def pyspark_read_json(bucket: str, arquivo: str):
    '''
    Lê um arquivo JSON de um bucket no sistema Minio usando o Apache Spark.

    Args:
        bucket (str): O nome do bucket no sistema Minio.
        arquivo (str): O nome do arquivo JSON a ser lido.
    '''
    url = f"s3a://{bucket}/{arquivo}"
    df = spark.read.option('header', 'true').option('inferSchema', 'true').json(url)
    return df

def pyspark_write_parquet_overwrite(df, bucket: str, arquivo: str):
    '''
    Escreve um DataFrame Spark em formato Parquet, sobrescrevendo o arquivo existente, em um bucket no sistema Minio.

    Args:
        df (DataFrame): O DataFrame Spark a ser gravado em formato Parquet.
        bucket (str): O nome do bucket no sistema Minio.
        arquivo (str): O nome do arquivo Parquet a ser gravado.

    Note:
        Certifique-se de que a sessão Spark (variável 'spark') esteja configurada e disponível antes de chamar esta função.
    '''
    url = f"s3a://{bucket}/{arquivo}"
    df.write.mode("overwrite").parquet(url)
    


#Arquivos CSVs
df_annual = pyspark_read_csv('bronze', 'annual-enterprise-2021.csv' )
df_annual = df_annual.withColumn("Value", col("Value").cast("float"))
df_research = pyspark_read_csv('bronze', 'research.csv')

#Arquivos JSON
df_cores = pyspark_read_json('bronze', 'cores.json')
df_cores = df_cores.withColumn('_id', col('_id').getField('$oid'))

#Arquivo JSON em formato que o pyspark nao consegue ler, irei tratar através do json
download_minio('api_teste.json', 'bronze', './api.json')
with open("./api.json", "r") as arquivo:
    api_json = json.load(arquivo)
api_json = api_json['bpi']
df_api_json = pd.DataFrame(api_json).T
df_api_json = spark.createDataFrame(df_api_json)

#Escrevendo DataFrame no objectstorage no minio
pyspark_write_parquet_overwrite(df_annual, 'gold', 'annual_enterprise_2021')
pyspark_write_parquet_overwrite(df_research, 'gold', 'research')
pyspark_write_parquet_overwrite(df_cores, 'gold', 'cores')
pyspark_write_parquet_overwrite(df_api_json, 'gold', 'api_json')