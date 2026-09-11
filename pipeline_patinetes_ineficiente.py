"""
PIPELINE PATINETES — INEFICIENTE (baseline)
Projeto: Simulador IoT de Patinetes Elétricos
Disciplina: Grafos e Compiladores — Sprint 4 (PySpark)

Objetivo do pipeline:
    Para cada leitura de GPS gerada pelo gerar.py (output.csv), encontrar a
    estação (estacoes.csv) mais próxima e contar quantas vezes cada patinete
    passou a menos de 300m de cada estação durante o horário de pico
    (12h às 14h).

    Isto substitui a função get_closest_spots() do gerar.py, que fazia essa
    mesma conta com um laço Python aninhado (O(leituras x estações), sem
    paralelismo, sem otimizador).

ATENÇÃO: este pipeline funciona, mas está de propósito mal otimizado,
para servir de baseline de comparação.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("PatinetesIneficiente") \
    .master("local[*]") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# AQE desligado e broadcast automático desligado de propósito, para os
# gargalos ficarem visíveis (mesmo esquema usado pela professora).
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

print("\n=== Spark UI: http://localhost:4040 ===\n")

inicio = time.time()

# ---------- 1. Leitura dos dados ----------
# ignoreLeadingWhiteSpace: o gerar.py escreve o cabeçalho como
# "id_patinete, lat, lon, dh" (com espaço depois da vírgula), então sem essa
# opção o Spark leria os nomes das colunas com espaço na frente.
leituras = spark.read.csv(
    "dados/output.csv", header=True, inferSchema=True,
    ignoreLeadingWhiteSpace=True
)
estacoes = spark.read.csv(
    "dados/estacoes.csv", header=True, inferSchema=True,
    ignoreLeadingWhiteSpace=True
)

leituras = leituras.withColumn("id_leitura", F.monotonically_increasing_id())

# ---------- 2. Repartition desnecessário logo na entrada ----------
# PROBLEMA 1: sem motivo nenhum antes do cross join, só gera shuffle extra.
leituras = leituras.repartition(50)

# ---------- 3. Cross join leituras x estações (SEM broadcast) ----------
# PROBLEMA 2: estacoes.csv tem poucas dezenas/centenas de linhas (cabe fácil
# em memória), mas como autoBroadcastJoinThreshold está em -1, o Spark faz
# um CartesianProduct completo, redistribuindo as duas tabelas pelo cluster
# em vez de simplesmente replicar a tabela pequena para todos os executors.
combinado = leituras.crossJoin(estacoes)

# ---------- 4. Distância haversine (funções nativas do Spark SQL) ----------
R_TERRA_KM = 6371.0088
lat1, lon1 = F.radians(leituras["lat"]), F.radians(leituras["lon"])
lat2, lon2 = F.radians(estacoes["lat"]), F.radians(estacoes["lon"])
dlat, dlon = lat2 - lat1, lon2 - lon1
a = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
distancia_km = 2 * R_TERRA_KM * F.asin(F.sqrt(a))

combinado = combinado.withColumn("distancia_km", distancia_km)

# ---------- 5. Encontrar a estação mais próxima (window function) ----------
# Isso roda em cima de TODAS as leituras (24h por dia), mesmo que só nos
# interesse o horário de pico — o filtro ainda não foi aplicado.
janela = Window.partitionBy("id_leitura").orderBy("distancia_km")
mais_proxima = combinado.withColumn("rank", F.row_number().over(janela)) \
                        .filter(F.col("rank") == 1)

# ---------- 6. Filtro aplicado só agora (tarde demais) ----------
# PROBLEMA 3: o filtro de horário de pico e de distância máxima deveria ter
# sido aplicado ANTES do cross join, para reduzir o volume de leituras que
# passam pelo passo mais caro do pipeline (o passo 3/4/5 acima).
filtrado = mais_proxima.filter(
    (F.col("distancia_km") < 0.3) &
    (F.hour(F.col("dh")).between(12, 14))
)

# ---------- 7. Agregação final ----------
resultado = filtrado.groupBy(
        F.col("id").alias("id_estacao_proxima"), "id_patinete"
    ).agg(
        F.count("*").alias("qtd_leituras_proximas"),
        F.min("distancia_km").alias("menor_distancia_km"),
    )

# ---------- 8. Ação (dispara a execução) ----------
resultado.orderBy(F.desc("qtd_leituras_proximas")).show(10)

fim = time.time()
print(f"\n>>> Tempo total de execução: {fim - inicio:.2f} segundos\n")

print(">>> Spark UI ativa em http://localhost:4040 (rode com: python3 -i)")
