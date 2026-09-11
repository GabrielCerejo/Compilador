"""
PIPELINE PATINETES — AQE LIGADO, SEM OTIMIZAÇÃO MANUAL
Projeto: Simulador IoT de Patinetes Elétricos
Disciplina: Grafos e Compiladores — Sprint 4 (PySpark)

Mesma estrutura "ingênua" do pipeline_patinetes_ineficiente.py (repartition
inútil, sem broadcast manual, filtro tarde), mas com o Adaptive Query
Execution (AQE) do Spark ligado, para ver o que o otimizador em tempo de
execução consegue corrigir sozinho (ex: converter o join em broadcast join
automaticamente, coalescer partições pequenas geradas pelo shuffle).

    arquivo                              AQE   otimização manual
    -----------------------------------  ----  -----------------
    pipeline_patinetes_ineficiente.py    off   não
    pipeline_patinetes_otimizado.py      off   sim
    pipeline_patinetes_aqe.py            on    não   <-- este
    pipeline_patinetes_aqe_otimizado.py  on    sim
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("PatinetesAQE") \
    .master("local[*]") \
    .config("spark.ui.port", "4042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# AQE ligado, com o autoBroadcastJoinThreshold no padrão do Spark (~10MB).
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("\n=== Spark UI: http://localhost:4042 ===\n")

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

# Mesmo "erro" de propósito do pipeline ineficiente: repartition sem motivo.
leituras = leituras.repartition(50)

# Cross join sem broadcast manual — deixamos o AQE decidir em tempo de
# execução se reescreve isso como broadcast, já que agora ele está ligado.
combinado = leituras.crossJoin(estacoes)

# ---------- Distância haversine (funções nativas do Spark SQL) ----------
R_TERRA_KM = 6371.0088
lat1, lon1 = F.radians(leituras["lat"]), F.radians(leituras["lon"])
lat2, lon2 = F.radians(estacoes["lat"]), F.radians(estacoes["lon"])
dlat, dlon = lat2 - lat1, lon2 - lon1
a = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
distancia_km = 2 * R_TERRA_KM * F.asin(F.sqrt(a))

combinado = combinado.withColumn("distancia_km", distancia_km)

janela = Window.partitionBy("id_leitura").orderBy("distancia_km")
mais_proxima = combinado.withColumn("rank", F.row_number().over(janela)) \
                        .filter(F.col("rank") == 1)

# Filtro ainda aplicado depois do cross join (não corrigido manualmente) —
# só o AQE está ativo aqui, não a reordenação manual do filtro.
filtrado = mais_proxima.filter(
    (F.col("distancia_km") < 0.3) &
    (F.hour(F.col("dh")).between(12, 14))
)

resultado = filtrado.groupBy(
        F.col("id").alias("id_estacao_proxima"), "id_patinete"
    ).agg(
        F.count("*").alias("qtd_leituras_proximas"),
        F.min("distancia_km").alias("menor_distancia_km"),
    )

resultado.orderBy(F.desc("qtd_leituras_proximas")).show(10)

fim = time.time()
print(f"\n>>> Tempo total de execução: {fim - inicio:.2f} segundos\n")

print(">>> Spark UI ativa em http://localhost:4042 (rode com: python3 -i)")
