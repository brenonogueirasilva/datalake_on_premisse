# Projeto: Data Lake on Premisse

## Introdução

Na era do Big Data, com o surgimento dos famosos três "V's" (Velocidade, Variedade e Volume), tornou-se imperativo o desenvolvimento de uma nova arquitetura de armazenamento de dados capaz de lidar com esse cenário desafiador. Nesse contexto, o Data Lake emergiu como uma das principais abordagens de armazenamento de dados concebidas para atender às demandas do Big Data. 

Com o intuito de aprofundar o entendimento do Data Lake e compreender de maneira prática o seu funcionamento, surgiu a ideia deste projeto que visa criar um Data Lake on-premises. Esse Data Lake simulará as três principais camadas de um Data Lake (Silver, Bronze e Gold), juntamente com as etapas de ingestão de dados e processamento. 

## Tecnologias Utilizadas

- **Minio**: Essa é a camada de armazenamento do Data Lake e equivale ao serviço S3 da AWS. O Minio oferece armazenamento de objetos, mas de forma local, proporcionando uma infraestrutura de armazenamento confiável. 
- **Hive Metastore**: O Hive Metastore atua como o catálogo de dados do Data Lake, sendo o local onde os metadados dos arquivos armazenados são mantidos e gerenciados. Esses metadados são fundamentais para organizar e consultar os dados de maneira eficiente. 
- **Trino**: O Trino é a camada do Data Lake responsável por consultas e pesquisas. Usando os metadados do Hive Metastore, o Trino permite realizar consultas aos dados armazenados no Data Lake. É o equivalente ao Amazon Athena da AWS, proporcionando uma interface para explorar e analisar os dados. 
- **Python**: A linguagem de programação Python desempenha um papel central neste projeto, sendo utilizada para extrair e processar os dados. Sua versatilidade e recursos tornam-no uma escolha sólida para as tarefas de manipulação de dados. 
- **Pyspark**: O Pyspark é um framework de processamento de Big Data que complementa o Python. Essa combinação permite trabalhar eficazmente com grandes volumes de dados e aplicar operações complexas de processamento. 
- **Docker**: O Docker é uma tecnologia fundamental para este projeto, pois é usada para criar contêineres que hospedam o ambiente necessário para suportar a infraestrutura do Data Lake. Essa abordagem de contêineres facilita o gerenciamento e a implantação dos componentes do projeto.

<p align="left">
<img src="/img/minio-logo.webp" alt="minio" height="50" /> 
<img src="/img/hive-logo.jpg" alt="hive" height="50" /> 
<img src="/img/trino-logo.png" alt="trino" height="50"/> 
<img src="/img/python-logo.png" alt="python" height="50"/> 
<img src="/img/pyspark.jpg" alt="pyspark" height="50"/> 
<img src="/img/docker-logo.png" alt="docker" height="50"/> 
</p>

## Arquitetura

![DataLake on Premisse](/img/datalake_on_premisse.png)

## Etapas do Projeto

### 1. Configuração do Ambiente
O início deste projeto envolve a criação de uma infraestrutura, e para essa finalidade, aproveitamos o poder do Docker. Com o Docker, conseguimos criar containers para cada componente fundamental no sistema de Data Lake, incluindo a camada de armazenamento (Minio), a camada de processamento (equipada com Python e o framework Pyspark), o catálogo de metadados (Hive Metastore), o banco de dados MariaDB (necessário para o funcionamento do Hive Metastore) e, por fim, o mecanismo de consulta (Trino).

Para configurar a infraestrutura, começamos criando um [Dockerfile](Dockerfile) que gera uma imagem personalizada contendo Python com a biblioteca Pyspark, bem como o Java, um pré-requisito para o Spark, além de outras bibliotecas essenciais para o desenvolvimento da lógica do projeto.

Com base nessa imagem personalizada e outras imagens já disponíveis no Docker Hub, configuramos todo o ambiente utilizando o [docker-compose.yml ](docker-compose.yml ), na quel é possível definir e orquestrar os componentes do sistema, além de configurar os parâmetros necessários para criar o ambiente e a infraestrutura ideais para o desenvolvimento do projeto.


### 2. Ingestão de Dados

Após a configuração bem-sucedida do ambiente, o próximo passo é a ingestão de dados na camada de armazenamento. Para realizar essa tarefa, fazemos uso do Python em conjunto com funções da biblioteca Minio, que nos habilita a interagir com esse ambiente de armazenamento. Normalmente, um Data Lake é organizado em três camadas principais: Bronze (onde os dados de ingestão brutos são armazenados sem processamento), Silver (uma espécie de camada intermediária onde os dados são processados, mas ainda não estão prontos para consumo), e Gold (a camada refinada na qual os dados estão prontos para serem consumidos por analistas e cientistas de dados, por exemplo). Sendo assim, foram criados buckets que representam cada uma dessas camadas no Minio.

Para simular a ingestão de dados no Data Lake, utilizamos várias fontes de dados distintas comumente encontradas no mercado de trabalho, incluindo:
    • Arquivos locais CSV;
    • Bancos de dados relacionais PostgreSQL;
    • Bancos de dados NoSQL MongoDB;
    • Retornos de APIs;
    • Arquivos HTML, com a finalidade de posteriormente realizar web scraping.

Com base nisso, criamos o script [camada_ingestao.py](/src/camada_ingestao.py), no qual implementamos a inserção dos dados provenientes de diversas fontes na camada Bronze do Data Lake, ou seja, no bucket Bronze do Minio. Este é um passo crucial para preparar os dados e torná-los disponíveis para processamento posterior e, eventualmente, consumo pelas equipes de análise e cientistas de dados.


### 3. Processamento dos Dados

É prática comum no Data Lake que os dados prontos para consumo estejam armazenados na camada Gold. Esses dados costumam ser representados no formato Parquet, pois esse tipo de arquivo é otimizado para o processamento de grandes volumes de dados. Ele se destaca por sua capacidade de compressão eficiente em comparação a outros formatos. Portanto, a etapa de processamento dos dados envolve uma série de operações:

1. Leitura de Dados pelo PySpark: Os dados na camada Bronze são lidos pelo PySpark, um poderoso framework de processamento de Big Data, com a ajuda de conectores que se comunicam com o Minio, onde os dados são armazenados.
2. Conversão para Formato Parquet: Os dados lidos são convertidos para o formato Parquet. Essa conversão é realizada com a ajuda do PySpark, que é altamente eficiente no tratamento desse tipo de operação.
3. Escrita dos Dados na Camada Gold: Por fim, os dados são escritos novamente no Data Lake, mas agora na camada Gold. Essa é a camada na qual os dados estão prontos para serem consumidos por analistas, cientistas de dados e outras equipes.

Tais etapas anteriores foram feitas através do script [processamento.py](/src/processamento.py) .

### 4. Catálogo de Dados

Após os dados estarem prontos para consulta no Data Lake, é necessário executar consultas no Trino que sejam capazes de gerar os metadados das tabelas no Lake. Esses metadados são, então, salvos no Hive Metastore, que atua como um catálogo de dados, contendo informações sobre a estrutura e localização dos dados armazenados no Data Lake.

O processo de catalogação envolve a leitura dos metadados das tabelas presentes na camada Gold do Data Lake. Por meio desses metadados, como os esquemas das tabelas que são armazenados em arquivos Parquet, é possível gerar consultas no Trino. Essas consultas têm como objetivo criar as tabelas no catálogo do Hive Metastore. Como resultado, essas tabelas passam a ser visíveis e acessíveis no próprio Trino, onde podem ser consultadas e utilizadas para análises e consultas de dados.

Para realizar esse processo, desenvolvemos o script [catalogo.py](/src/catalogo.py), que atua como uma espécie de "crawler." Ele faz a leitura dos metadados das tabelas presentes na camada Gold do Data Lake. Com base nesses metadados, que incluem informações sobre o esquema das tabelas, armazenadas em arquivos Parquet, o script constrói consultas que serão executadas no Trino. Essas consultas são executadas no Trino, o que resulta na criação dessas tabelas no catálogo do Hive Metastore. Isso possibilita que as tabelas sejam posteriormente consultadas e utilizadas no Trino para análises e consultas de dados.

Essa etapa de catalogação é fundamental para permitir a integração dos metadados das tabelas com a camada de processamento, viabilizando consultas e análises eficientes sobre os dados armazenados no Data Lake.

**Observação:** O projeto concentra-se no desenvolvimento da lógica e infraestrutura de um datalake e não necessariamente na segurança. Para simplificar o código, não serão criadas variáveis de ambiente para as credenciais, que, em vez disso, serão diretamente incorporadas no código.

### Referência 
https://github.com/hnawaz007/pythondataanalysis/tree/main/data-lake
