FROM jupyter/pyspark-notebook:spark-3.3.1 

COPY ./requirements.txt ./
RUN pip3 install --no-cache-dir -r ./requirements.txt 
