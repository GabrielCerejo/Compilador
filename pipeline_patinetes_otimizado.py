"""
PIPELINE PATINETES — OTIMIZAÇÃO MANUAL (AQE desligado)
Projeto: Simulador IoT de Patinetes Elétricos
Disciplina: Grafos e Compiladores — Sprint 4 (PySpark)

Mesmo resultado do pipeline_patinetes_ineficiente.py, mas com as
otimizações feitas à mão pelo programador:
    - filtro (horário de pico) aplicado ANTES do cross join
    - broadcast explícito da tabela pequena (estações)
    - sem repartition inútil

AQE continua desligado aqui de propósito, para isolar o ganho que veio
só da otimização manual (comparar com pipeline_patinetes_aqe_otimizado.py,
onde AQE também está ligado).

    arquivo                              AQE   otimização manual
    -----------------------------------  ----  -----------------
    pipeline_patinetes_ineficiente.py    off   não
    pipeline_patinetes_otimizado.py      off   sim   <-- este
    pipeline_patinetes_aqe.py            on    não
    pipeline_patinetes_aqe_otimizado.py  on    sim
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("PatinetesOtimizado") \
    .master("local[*]") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# AQE continua desligado de propósito (ver docstring acima).
spark.conf.set("spark.sql.adaptive.enabled", "false")

print("\n=== Spark UI: http://localhost:4041 ===\n")

inicio = time.time()

# ---------- 1. Leitura dos dados ----------
leituras = spark.read.csv(
    "dados/output.csv", header=True, inferSchema=True,
    ignoreLeadingWhiteSpace=True
)
estacoes = spark.read.csv(
    "dados/estacoes.csv", header=True, inferSchema=True,
    ignoreLeadingWhiteSpace=True
)

leituras = leituras.withColumn("id_leitura", F.monotonically_increasing_id())

# OTIMIZAÇÃO MANUAL 1: filtrar horário de pico ANTES do cross join.
# Isso reduz drasticamente o número de linhas que entram no passo mais caro
# (o cross join x cálculo de distância), já que só ~2 das 24h por dia
# interessam. Sem repartition(50): aquele shuffle inicial inútil não existe.
leituras_pico = leituras.filter(F.hour(F.col("dh")).between(12, 14))

# ---------- 2. Cross join com broadcast manual ----------
# OTIMIZAÇÃO MANUAL 2: broadcast explícito da tabela de estações (pequena).
# O Spark replica essa tabela inteira para cada executor em vez de fazer
# shuffle nela — cross join vira um BroadcastNestedLoopJoin em vez de um
# CartesianProduct completo.
combinado = leituras_pico.crossJoin(F.broadcast(estacoes))

# ---------- 3. Distância haversine (funções nativas do Spark SQL) ----------
R_TERRA_KM = 6371.0088
lat1, lon1 = F.radians(leituras_pico["lat"]), F.radians(leituras_pico["lon"])
lat2, lon2 = F.radians(estacoes["lat"]), F.radians(estacoes["lon"])
dlat, dlon = lat2 - lat1, lon2 - lon1
a = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
distancia_km = 2 * R_TERRA_KM * F.asin(F.sqrt(a))

combinado = combinado.withColumn("distancia_km", distancia_km)

# ---------- 4. Estação mais próxima + filtro de distância já aplicados sobre
# um volume bem menor de dados (só horário de pico) ----------
janela = Window.partitionBy("id_leitura").orderBy("distancia_km")
mais_proxima = combinado.withColumn("rank", F.row_number().over(janela)) \
                        .filter(F.col("rank") == 1)

filtrado = mais_proxima.filter(F.col("distancia_km") < 0.3)

# ---------- 5. Agregação final ----------
resultado = filtrado.groupBy(
        F.col("id").alias("id_estacao_proxima"), "id_patinete"
    ).agg(
        F.count("*").alias("qtd_leituras_proximas"),
        F.min("distancia_km").alias("menor_distancia_km"),
    )

# ---------- 6. Ação (dispara a execução) ----------
resultado.orderBy(F.desc("qtd_leituras_proximas")).show(10)

fim = time.time()
print(f"\n>>> Tempo total de execução: {fim - inicio:.2f} segundos\n")

print(">>> Spark UI ativa em http://localhost:4041 (rode com: python3 -i)")
