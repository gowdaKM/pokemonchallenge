FROM ubuntu:latest

# Install Java
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# Install Python and pip
RUN apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python 


#install library for kaggle creds 

RUN pip install kaggle

#RUN cp kaggle.json ~/.kaggle/
#RUN chmod 600 ~/.kaggle/kaggle.json 
#kaggle competitions download -c pokemon



RUN mkdir -p /root/pokemon/input
RUN mkdir -p /root/.kaggle 
COPY kaggle.json /root/.kaggle/
COPY takehomechallenge.py /root/pokemon/
RUN chmod 600 /root/.kaggle/kaggle.json


RUN kaggle datasets download -d abcsds/pokemon  -f Pokemon.csv
RUN mv Pokemon.csv /root/pokemon/input/Pokemon.csv 


COPY spark-3.2.0-bin-hadoop3.2.tgz /tmp/spark-3.2.0-bin-hadoop3.2.tgz

RUN tar xzf /tmp/spark-3.2.0-bin-hadoop3.2.tgz -C /usr/local && \
    mv /usr/local/spark-3.2.0-bin-hadoop3.2 /usr/local/spark && \
    rm /tmp/spark-3.2.0-bin-hadoop3.2.tgz

# Set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
