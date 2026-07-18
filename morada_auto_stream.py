#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Morada Automatic Stream Pipeline

Este script automatiza 100% da infraestrutura do Apache Kafka e PySpark
no Google Colab ou em ambiente local Linux. 

Passos executados automaticamente:
1. Verifica e instala dependências de Python (pyspark, kafka-python-ng).
2. Baixa e extrai os binários do Apache Kafka (se necessário).
3. Inicia o Zookeeper e o Broker Kafka em processos secundários.
4. Aguarda a abertura das portas de rede (2181 e 9092) para garantir prontidão.
5. Cria o tópico 'morada-bookings'.
6. Dispara o produtor de simulação de reservas em tempo real (Thread).
7. Inicializa a SparkSession e o Structured Streaming.
8. Consome, transforma (LGPD, limpeza, cashback) e imprime os dados na tela.
9. Encerra todos os subprocessos graciosamente no cancelamento (Ctrl+C).
"""

import os
import sys
import time
import socket
import urllib.request
import tarfile
import subprocess
import threading
import json
import random
from datetime import datetime

# ==========================================
# 1. FUNÇÕES AUXILIARES DE INFRAESTRUTURA
# ==========================================
def is_port_open(port):
    """Verifica se uma porta local está aberta para conexão."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(('127.0.0.1', port)) == 0

def setup_python_dependencies():
    """Instala programaticamente dependências que estejam faltando."""
    try:
        import pyspark
        from kafka import KafkaProducer
        print("-> Dependências Python já instaladas.")
    except ImportError:
        print("-> Dependências ausentes. Instalando PySpark e Kafka-Python...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark==3.5.0", "kafka-python-ng"])
        print("-> Instalação concluída com sucesso.")

def setup_kafka_binaries():
    """Baixa e descompacta o Kafka na pasta local caso não exista."""
    kafka_dir = "kafka_2.12-3.5.1"
    kafka_tgz = "kafka_2.12-3.5.1.tgz"
    
    if not os.path.exists(kafka_dir):
        print(f"-> Binários do Kafka não encontrados. Baixando de archive.apache.org...")
        url = f"https://archive.apache.org/dist/kafka/3.5.1/{kafka_tgz}"
        urllib.request.urlretrieve(url, kafka_tgz)
        
        print("-> Extraindo arquivos...")
        with tarfile.open(kafka_tgz, "r:gz") as tar:
            tar.extractall()
            
        os.remove(kafka_tgz)
        print("-> Instalação dos binários do Kafka concluída.")
    else:
        print("-> Binários do Kafka já disponíveis localmente.")

# ==========================================
# 2. FLUXO PRINCIPAL DE EXECUÇÃO
# ==========================================
def main():
    print("====================================================")
    print("          MORADA AUTO STREAM PIPELINE - START       ")
    print("====================================================")
    
    # 2.1 Instalar e configurar dependências
    setup_python_dependencies()
    setup_kafka_binaries()
    
    # Configurar JAVA_HOME para o Spark
    if "JAVA_HOME" not in os.environ:
        # Tenta caminhos padrão no Colab/Linux
        for path in ["/usr/lib/jvm/java-8-openjdk-amd64", "/usr/lib/jvm/java-11-openjdk-amd64", "/usr/lib/jvm/java-17-openjdk-amd64"]:
            if os.path.exists(path):
                os.environ["JAVA_HOME"] = path
                print(f"-> JAVA_HOME configurado para: {path}")
                break
        else:
            print("[WARN] JAVA_HOME não configurado e caminho padrão não localizado. O Spark pode falhar.")

    zookeeper_process = None
    kafka_process = None
    
    try:
        # 2.2 Iniciar Zookeeper
        if not is_port_open(2181):
            print("-> Iniciando Zookeeper...")
            zookeeper_process = subprocess.Popen(
                ["./kafka_2.12-3.5.1/bin/zookeeper-server-start.sh", "./kafka_2.12-3.5.1/config/zookeeper.properties"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            print("-> Aguardando Zookeeper subir na porta 2181...")
            while not is_port_open(2181):
                time.sleep(1)
            print("-> Zookeeper está pronto!")
        else:
            print("-> Zookeeper já está rodando na porta 2181.")
            
        # 2.3 Iniciar Kafka Broker
        if not is_port_open(9092):
            print("-> Iniciando Broker Kafka...")
            kafka_process = subprocess.Popen(
                ["./kafka_2.12-3.5.1/bin/kafka-server-start.sh", "./kafka_2.12-3.5.1/config/server.properties"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            print("-> Aguardando Broker Kafka subir na porta 9092...")
            while not is_port_open(9092):
                time.sleep(1)
            print("-> Broker Kafka está pronto!")
        else:
            print("-> Broker Kafka já está rodando na porta 9092.")
            
        # 2.4 Criar Tópico do Kafka
        print("-> Verificando/Criando tópico 'morada-bookings'...")
        subprocess.run([
            "./kafka_2.12-3.5.1/bin/kafka-topics.sh", "--create", 
            "--bootstrap-server", "localhost:9092", 
            "--replication-factor", "1", "--partitions", "1", 
            "--topic", "morada-bookings", "--if-not-exists"
        ], stdout=subprocess.DEVNULL)
        
        # Importações tardias pós-instalação de pacotes
        from kafka import KafkaProducer
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, from_json, regexp_replace, when, expr
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

        # 2.5 Iniciar Produtor Fictício de Eventos em background Thread
        def run_producer():
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            nomes_br = ["Ana Souza", "Bruno Lima", "Carlos Silva", "Daniela Costa", "Eduardo Pereira"]
            pagamentos = ["pix", "credit_card", "split_bill"]
            
            while True:
                booking_event = {
                    "id": str(random.randint(100000, 999999)),
                    "listing_id": str(random.randint(1, 50)),
                    "primary_guest_name": random.choice(nomes_br),
                    "primary_guest_cpf": f"{random.randint(100,999)}.{random.randint(100,999)}.{random.randint(100,999)}-{random.randint(10,99)}",
                    "primary_guest_phone": f"+55 (11) 9{random.randint(8000,9999)}-{random.randint(1000,9999)}",
                    "total_price": float(round(random.uniform(300.0, 2000.0), 2)),
                    "payment_type": random.choice(pagamentos),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                producer.send('morada-bookings', booking_event)
                time.sleep(random.uniform(2.0, 4.0)) # Envia reserva a cada 2-4 segs

        prod_thread = threading.Thread(target=run_producer, daemon=True)
        prod_thread.start()
        print("-> Simulador de reservas (Produtor Kafka) iniciado em segundo plano.")

        # 2.6 Configurar e Iniciar Spark Streaming
        print("-> Inicializando SparkSession com suporte ao Kafka...")
        spark = SparkSession.builder \
            .appName("MoradaAutoStreamingETL") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
            
        booking_schema = StructType([
            StructField("id", StringType(), True),
            StructField("listing_id", StringType(), True),
            StructField("primary_guest_name", StringType(), True),
            StructField("primary_guest_cpf", StringType(), True),
            StructField("primary_guest_phone", StringType(), True),
            StructField("total_price", DoubleType(), True),
            StructField("payment_type", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        print("-> Estabelecendo leitura contínua (Stream) do Kafka...")
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "morada-bookings") \
            .option("startingOffsets", "latest") \
            .load()

        # Parseamento e Transformações (LGPD, Fone, Cashback)
        df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json_payload") \
            .select(from_json(col("json_payload"), booking_schema).alias("data")) \
            .select("data.*")
            
        df_clean_cpf = df_parsed.withColumn("cpf_raw", regexp_replace(col("primary_guest_cpf"), r"[.-]", ""))
        
        df_transformed = df_clean_cpf.withColumn(
            "masked_cpf",
            expr("concat('***.', substring(cpf_raw, 4, 3), '.', substring(cpf_raw, 7, 3), '-**')")
        ).withColumn(
            "phone_clean",
            regexp_replace(col("primary_guest_phone"), r"[^\d+]", "")
        ).withColumn(
            "earned_cashback",
            when(col("payment_type") == "pix", col("total_price") * 0.02)
            .otherwise(col("total_price") * 0.01)
        ).select("timestamp", "id", "primary_guest_name", "masked_cpf", "phone_clean", "payment_type", "total_price", "earned_cashback")

        # Escrever saída da stream no Console para visualização em tempo real
        print("-> Iniciando a gravação no console do streaming...")
        query = df_transformed.writeStream \
            .outputMode("append") \
            .format("console") \
            .trigger(processingTime="5 seconds") \
            .start()

        # Mantém o script rodando até o usuário interromper
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n[INFO] Cancelamento manual detectado pelo usuário (Ctrl+C).")
    except Exception as e:
        print(f"\n[ERROR] Ocorreu uma falha na execução: {e}")
    finally:
        print("====================================================")
        print("-> Finalizando recursos e processos...")
        
        # Desliga processos do Kafka se foram iniciados por este script
        if kafka_process:
            print("-> Parando Broker Kafka...")
            kafka_process.terminate()
            kafka_process.wait()
        if zookeeper_process:
            print("-> Parando Zookeeper...")
            zookeeper_process.terminate()
            zookeeper_process.wait()
            
        print("-> Todos os serviços foram desligados com sucesso!")
        print("====================================================")

if __name__ == "__main__":
    main()
