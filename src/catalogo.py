from trino.dbapi import connect

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from minio import Minio
from minio.error import S3Error

def select_trino(sql: str):
    '''
    Executa uma consulta SQL no Trino (anteriormente Presto) usando a configuração especificada.
    Args:
        sql (str): A consulta SQL a ser executada.
    '''
    conn = connect(
        host="trino",
        port=8080,
        user="trino",
        catalog="minio",
        schema="gold",
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        print(row)


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


minio_client = Minio(
    "minio:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False  
)
def list_objects_buckets_minio(nome_bucket: str):
    '''
    Lista todos os objetos em um bucket Minio.

    Args:
        nome_bucket (str): O nome do bucket Minio.

    Returns:
        list: Uma lista de nomes de objetos no bucket.

    Note:
        Certifique-se de que a configuração do cliente Minio esteja corretamente definida antes de chamar esta função.
    '''
    try:
        objects = minio_client.list_objects(nome_bucket, recursive=True)
        ls_objects = []
        for obj in objects:
            ls_objects.append(obj.object_name)
        return ls_objects
    except S3Error as err:
        print(err)


def read_parquet_schema(bucket: str, arquivo: str):
    '''
    Lê o esquema (schema) de um arquivo Parquet em um bucket Minio usando o Apache Spark.
    Args:
        bucket (str): O nome do bucket Minio.
        arquivo (str): O nome do arquivo Parquet contendo o esquema.
    '''
    url = f"s3a://{bucket}/{arquivo}"
    df_metadados = spark.read.parquet(url).limit(1)
    return df_metadados

def gera_catalogo(tabela: str, schema: str):
    '''
    Gera uma consulta SQL para criar uma tabela Minio baseada no esquema de um arquivo Parquet.
    Args:
        tabela (str): O nome da tabela Minio a ser criada.
        schema (str): O esquema da tabela.
    '''
    df = read_parquet_schema('gold', tabela)
    esquema = df.schema
    esquema = list(map(lambda x : [x.name, x.dataType ], esquema ))
    esquema = list(map(lambda x : str(x[0]) + " " + str(x[1]).replace('Type()', '').lower() + ',' , esquema))
    renomear_colunas = {'string' : 'varchar', 'integer' : 'int', 'float' : 'double'}
    for linha in range(len(esquema)):
        for chave, valor in renomear_colunas.items():
            esquema[linha] = esquema[linha].replace(chave, valor)
    cabecalho = f"CREATE TABLE IF NOT EXISTS minio.{schema}.{tabela} ("
    rodape = f"WITH ( external_location = 's3a://gold/{tabela}/', format = 'PARQUET') "
    consulta = ""
    for linha in esquema:
        consulta += linha + "\n"
    consulta = consulta[0: len(consulta)-2]
    consulta_final = cabecalho + "\n" + consulta + ") \n" + rodape
    return consulta_final

ls_object = list(map(lambda x : x.split('/')[0] , list_objects_buckets_minio('gold')))
ls_object = list(set(ls_object))
for elemento in ls_object:
    consulta = gera_catalogo(elemento, 'gold')
    select_trino(consulta)