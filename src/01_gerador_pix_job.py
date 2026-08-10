# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de eventos PIX — simula o produtor Kafka
# MAGIC
# MAGIC Este notebook faz o papel do time que publica o tópico `pix.eventos`.
# MAGIC Não é bronze nem silver — é a **origem**, fora do seu domínio.
# MAGIC
# MAGIC Escrita em **batch**, não streaming. Serverless não aceita trigger
# MAGIC contínuo, e gerador não precisa de stream: você diz quantos eventos
# MAGIC quer e ele escreve. Volume determinístico, custo previsível.
# MAGIC
# MAGIC **Setup do Job (Workflows → Create job):**
# MAGIC - Task type: Notebook → aponte para este arquivo
# MAGIC - Parameters: `num_eventos` controla o tamanho do lote
# MAGIC - Comece sem schedule; trigger manual enquanto estuda

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_leo_estudo_2")
dbutils.widgets.text("schema", "contas_digitais")
dbutils.widgets.text("num_eventos", "50000")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NUM_EVENTOS = int(dbutils.widgets.get("num_eventos"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## O tópico
# MAGIC
# MAGIC `pix_topic` tem exatamente as colunas que o source `kafka` do Spark
# MAGIC expõe. Quem consome não sabe se veio de broker ou de Delta.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("""
CREATE TABLE IF NOT EXISTS pix_topic (
    key        STRING,
    value      STRING,
    timestamp  TIMESTAMP,
    offset     BIGINT
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuidade de offset
# MAGIC
# MAGIC `spark.range()` sempre começa em 0. Sem esse deslocamento, cada
# MAGIC execução repetiria os mesmos `end_to_end_id` — e você acharia que a
# MAGIC dedupe da silver está com bug, quando o problema seria o gerador.

# COMMAND ----------

offset_base = (
    spark.table("pix_topic")
    .agg(F.coalesce(F.max("offset"), F.lit(-1)).alias("m"))
    .collect()[0]["m"]
) + 1

print(f"offset inicial deste lote: {offset_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerar o lote
# MAGIC
# MAGIC Skew plantado: ~40% do volume em 5 contas PJ (lojistas). É o
# MAGIC comportamento real do PIX e o que vai te dar problema em groupBy
# MAGIC e join mais pra frente.
# MAGIC
# MAGIC Os timestamps se espalham nas últimas 6 horas em vez de todos
# MAGIC caírem no mesmo instante — sem isso o watermark e o particionamento
# MAGIC por data da silver não teriam nada pra fazer.

# COMMAND ----------

base = spark.range(NUM_EVENTOS).withColumn("seq", F.col("id") + F.lit(offset_base))

conta_pagador = F.when(
    F.pmod(F.col("seq"), F.lit(10)) < 4,
    F.concat(F.lit("PJ-"), F.pmod(F.col("seq"), F.lit(5)).cast("string")),
).otherwise(
    F.concat(F.lit("PF-"), F.pmod(F.col("seq") * F.lit(7919), F.lit(50000)).cast("string"))
)

# espalha os eventos nas últimas 6 horas (21600 segundos)
ts_evento = F.current_timestamp() - F.expr(
    "make_interval(0, 0, 0, 0, 0, 0, pmod(seq * 13, 21600))"
)

# canal de origem do PIX — ~70% app, ~20% internet banking, ~10% API
canal = (
    F.when(F.pmod(F.col("seq"), F.lit(10)) < 7, F.lit("APP"))
    .when(F.pmod(F.col("seq"), F.lit(10)) < 9, F.lit("INTERNET_BANKING"))
    .otherwise(F.lit("API"))
)

eventos = (
    base.withColumn("pagador", conta_pagador)
    .withColumn("ts", ts_evento)
    .withColumn("canal", canal)
    .select(
        F.col("pagador").alias("key"),
        F.to_json(
            F.struct(
                F.concat(F.lit("E"), F.col("seq").cast("string")).alias("end_to_end_id"),
                F.col("pagador").alias("conta_pagador"),
                F.concat(
                    F.lit("PF-"),
                    F.pmod(F.col("seq") * F.lit(104729), F.lit(50000)).cast("string"),
                ).alias("conta_recebedor"),
                (F.pmod(F.col("seq") * F.lit(37), F.lit(500000)) / F.lit(100)).alias("valor"),
                F.when(F.pmod(F.col("seq"), F.lit(97)) == 0, F.lit("DEVOLVIDO"))
                .when(F.pmod(F.col("seq"), F.lit(53)) == 0, F.lit("REJEITADO"))
                .otherwise(F.lit("LIQUIDADO"))
                .alias("status"),
                F.col("canal").alias("canal"),
                F.col("ts").cast("string").alias("data_hora_evento"),
            )
        ).alias("value"),
        F.col("ts").alias("timestamp"),
        F.col("seq").alias("offset"),
    )
)

eventos.write.format("delta").mode("append").saveAsTable("pix_topic")

total = spark.table("pix_topic").count()
print(f"lote de {NUM_EVENTOS} gravado — {total} eventos no tópico")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(offset) AS menor, max(offset) AS maior, count(*) AS total
# MAGIC FROM pix_topic;
