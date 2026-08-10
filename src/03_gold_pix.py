# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — agregados de PIX por conta
# MAGIC
# MAGIC Lê a `silver_pix_transacoes` e produz a camada de consumo.
# MAGIC
# MAGIC Batch puro, sem streaming. Gold quase sempre é batch: o consumidor
# MAGIC (dashboard, feature de crédito, relatório) quer o fechamento do dia,
# MAGIC não o evento a evento.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_leo_estudo_2")
dbutils.widgets.text("schema", "contas_digitais")
dbutils.widgets.text("dias_retroativos", "7")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DIAS = int(dbutils.widgets.get("dias_retroativos"))

spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Janela de reprocessamento
# MAGIC
# MAGIC Gold não reprocessa a silver inteira todo dia — cara demais. Também
# MAGIC não processa só ontem: PIX devolvido ou liquidação atrasada chega
# MAGIC depois e muda o fechamento de dias anteriores.
# MAGIC
# MAGIC A saída é recalcular uma janela móvel (7 dias) e sobrescrever o
# MAGIC resultado por chave. Rodar duas vezes dá o mesmo número.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

corte = F.date_sub(F.current_date(), DIAS)

silver = spark.table("silver_pix_transacoes").filter(F.col("data_evento") >= corte)

print("linhas na janela:", silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 1 — movimentação diária por conta
# MAGIC
# MAGIC Uma conta aparece como pagadora e como recebedora. Em vez de dois
# MAGIC agregados separados, empilhamos os dois papéis e agrupamos uma vez só.
# MAGIC
# MAGIC É aqui que o skew morde: as contas PJ concentram ~40% do volume,
# MAGIC então algumas partições do `groupBy` ficam muito maiores que as
# MAGIC outras. Com 50k linhas não dói; com 500M, dói.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_pix_conta_diario (
    conta            STRING,
    data_evento      DATE,
    qtd_enviada      BIGINT,
    valor_enviado    DOUBLE,
    qtd_recebida     BIGINT,
    valor_recebido   DOUBLE,
    saldo_liquido    DOUBLE,
    ticket_medio     DOUBLE,
    _atualizado_em   TIMESTAMP
) USING DELTA PARTITIONED BY (data_evento)
""")

# COMMAND ----------

liquidadas = silver.filter(F.col("status") == "LIQUIDADO")

enviadas = liquidadas.select(
    F.col("conta_pagador").alias("conta"),
    "data_evento",
    "valor",
    F.lit("ENVIO").alias("papel"),
)

recebidas = liquidadas.select(
    F.col("conta_recebedor").alias("conta"),
    "data_evento",
    "valor",
    F.lit("RECEBIMENTO").alias("papel"),
)

movimentos = enviadas.unionByName(recebidas)

agregado = (
    movimentos.groupBy("conta", "data_evento")
    .agg(
        F.sum(F.when(F.col("papel") == "ENVIO", 1).otherwise(0)).alias("qtd_enviada"),
        F.sum(F.when(F.col("papel") == "ENVIO", F.col("valor")).otherwise(0.0)).alias("valor_enviado"),
        F.sum(F.when(F.col("papel") == "RECEBIMENTO", 1).otherwise(0)).alias("qtd_recebida"),
        F.sum(F.when(F.col("papel") == "RECEBIMENTO", F.col("valor")).otherwise(0.0)).alias("valor_recebido"),
    )
    .withColumn("saldo_liquido", F.round(F.col("valor_recebido") - F.col("valor_enviado"), 2))
    .withColumn(
        "ticket_medio",
        F.round(
            (F.col("valor_enviado") + F.col("valor_recebido"))
            / F.greatest(F.col("qtd_enviada") + F.col("qtd_recebida"), F.lit(1)),
            2,
        ),
    )
    .withColumn("valor_enviado", F.round("valor_enviado", 2))
    .withColumn("valor_recebido", F.round("valor_recebido", 2))
    .withColumn("_atualizado_em", F.current_timestamp())
)

alvo = DeltaTable.forName(spark, "gold_pix_conta_diario")
(
    alvo.alias("t")
    .merge(agregado.alias("s"), "t.conta = s.conta AND t.data_evento = s.data_evento")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("gold_pix_conta_diario:", spark.table("gold_pix_conta_diario").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 2 — resumo diário da operação
# MAGIC
# MAGIC Poucas linhas, alta leitura. É o que alimenta dashboard e o
# MAGIC acompanhamento de volumetria — inclusive pra detectar queda anômala
# MAGIC de transações, que costuma ser o primeiro sinal de incidente.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_pix_resumo_diario (
    data_evento        DATE,
    qtd_total          BIGINT,
    qtd_liquidada      BIGINT,
    qtd_rejeitada      BIGINT,
    qtd_devolvida      BIGINT,
    volume_liquidado   DOUBLE,
    contas_ativas      BIGINT,
    taxa_rejeicao_pct  DOUBLE,
    _atualizado_em     TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

resumo = (
    silver.groupBy("data_evento")
    .agg(
        F.count("*").alias("qtd_total"),
        F.sum(F.when(F.col("status") == "LIQUIDADO", 1).otherwise(0)).alias("qtd_liquidada"),
        F.sum(F.when(F.col("status") == "REJEITADO", 1).otherwise(0)).alias("qtd_rejeitada"),
        F.sum(F.when(F.col("status") == "DEVOLVIDO", 1).otherwise(0)).alias("qtd_devolvida"),
        F.round(
            F.sum(F.when(F.col("status") == "LIQUIDADO", F.col("valor")).otherwise(0.0)), 2
        ).alias("volume_liquidado"),
        F.countDistinct("conta_pagador").alias("contas_ativas"),
    )
    .withColumn(
        "taxa_rejeicao_pct",
        F.round(F.col("qtd_rejeitada") / F.col("qtd_total") * 100, 2),
    )
    .withColumn("_atualizado_em", F.current_timestamp())
)

alvo_resumo = DeltaTable.forName(spark, "gold_pix_resumo_diario")
(
    alvo_resumo.alias("t")
    .merge(resumo.alias("s"), "t.data_evento = s.data_evento")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("gold_pix_resumo_diario:", spark.table("gold_pix_resumo_diario").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality gate
# MAGIC
# MAGIC A checagem que importa na gold é **reconciliação com a silver**.
# MAGIC Agregado que não bate com a origem é o erro mais caro que existe em
# MAGIC dado financeiro: o número parece plausível, ninguém desconfia.

# COMMAND ----------

falhas = []

conf = spark.sql(f"""
SELECT
  (SELECT count(*) FROM silver_pix_transacoes
     WHERE data_evento >= date_sub(current_date(), {DIAS})) AS silver_janela,
  (SELECT coalesce(sum(qtd_total), 0) FROM gold_pix_resumo_diario
     WHERE data_evento >= date_sub(current_date(), {DIAS})) AS gold_janela,
  (SELECT count(*) FROM gold_pix_conta_diario
     WHERE qtd_enviada = 0 AND qtd_recebida = 0)            AS linhas_vazias
""").collect()[0]

if conf["silver_janela"] != conf["gold_janela"]:
    falhas.append(
        f"reconciliação: silver={conf['silver_janela']}, gold={conf['gold_janela']}"
    )

if conf["linhas_vazias"] > 0:
    falhas.append(f"{conf['linhas_vazias']} linhas sem movimento na gold")

print(f"silver_janela={conf['silver_janela']} gold_janela={conf['gold_janela']}")

if falhas:
    raise Exception("QUALITY GATE FALHOU:\n  - " + "\n  - ".join(falhas))

print("quality gate: OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_pix_resumo_diario ORDER BY data_evento DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT conta, data_evento, qtd_enviada, valor_enviado, saldo_liquido
# MAGIC FROM gold_pix_conta_diario
# MAGIC ORDER BY valor_enviado DESC LIMIT 15;
