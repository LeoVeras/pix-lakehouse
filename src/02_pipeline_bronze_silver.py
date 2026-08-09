# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline PIX — Bronze e Silver
# MAGIC
# MAGIC Consome o tópico produzido pelo job `01_gerador_pix_job`.
# MAGIC Este notebook não sabe nem se importa como o dado foi produzido —
# MAGIC é exatamente a relação que você terá com o time de plataforma.
# MAGIC
# MAGIC Todas as streams usam `availableNow`: processam o backlog que existir
# MAGIC e encerram. Único modo aceito em serverless, e o certo pra lab.
# MAGIC
# MAGIC **Parametrizado por widget** — nenhuma constante de ambiente no código.
# MAGIC É o que permite o mesmo notebook rodar em dev e prod mudando só a
# MAGIC config do job. Sem isso não existe deploy, existe cópia editada na mão.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_leo_estudo_2")
dbutils.widgets.text("schema", "contas_digitais")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("max_quarentena_pct", "1.0")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENV = dbutils.widgets.get("env")
MAX_QUARENTENA_PCT = float(dbutils.widgets.get("max_quarentena_pct"))

CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/ckpt"

spark.sql(f"USE {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ckpt")

print(f"env={ENV} catalog={CATALOG} schema={SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — cru, append-only, fiel à origem
# MAGIC
# MAGIC Regra de ouro: **não parseie nada aqui**. Guarde o payload como veio,
# MAGIC mais os metadados de ingestão. Se o schema do evento mudar ou você
# MAGIC descobrir um bug no parse, a Bronze te deixa reprocessar tudo sem
# MAGIC pedir replay pro time de plataforma.

# COMMAND ----------

from pyspark.sql import functions as F

topico = spark.readStream.table("pix_topic")

# --- para Kafka real, troque a linha acima por isto: ---
# topico = (
#     spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers", "broker:9092")
#     .option("subscribe", "pix.eventos")
#     .option("startingOffsets", "latest")
#     .load()
# )

bronze = topico.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("payload_raw"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("offset").alias("kafka_offset"),
    F.current_timestamp().alias("_ingest_ts"),
    F.lit("pix.eventos").alias("_source"),
).withColumn("_ingest_date", F.to_date("_ingest_ts"))

q_bronze = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/bronze_pix")
    .partitionBy("_ingest_date")
    .trigger(availableNow=True)
    .toTable("bronze_pix_eventos")
)

q_bronze.awaitTermination()
print("bronze:", spark.table("bronze_pix_eventos").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver — com quarentena
# MAGIC
# MAGIC O que mudou aqui é o que separa lab de produção: **payload ruim não
# MAGIC pode sumir em silêncio**. Antes, um JSON corrompido virava null no
# MAGIC `from_json` e era descartado por um `filter` — ninguém ficava sabendo.
# MAGIC
# MAGIC Agora cada micro-batch é dividido em dois destinos: o que passa vai
# MAGIC pra silver, o que falha vai pra `quarentena_pix` com o payload
# MAGIC original e o motivo. Você consegue investigar, corrigir e reprocessar.
# MAGIC
# MAGIC Dedupe em duas camadas, cada uma pegando um caso:
# MAGIC
# MAGIC 1. **`dropDuplicates` dentro do batch** — sem watermark, sem estado
# MAGIC    persistido. Pega repetição no mesmo lote.
# MAGIC 2. **`MERGE` por `end_to_end_id`** — pega repetição entre lotes e
# MAGIC    garante idempotência sob reprocessamento. Como o `end_to_end_id`
# MAGIC    é chave única por definição do PIX, essa garantia não tem prazo
# MAGIC    de validade (diferente de um watermark, que expira).

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta.tables import DeltaTable

schema_pix = StructType([
    StructField("end_to_end_id", StringType()),
    StructField("conta_pagador", StringType()),
    StructField("conta_recebedor", StringType()),
    StructField("valor", DoubleType()),
    StructField("status", StringType()),
    StructField("data_hora_evento", StringType()),
])

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_pix_transacoes (
    end_to_end_id     STRING,
    conta_pagador     STRING,
    conta_recebedor   STRING,
    valor             DOUBLE,
    status            STRING,
    data_hora_evento  TIMESTAMP,
    data_evento       DATE,
    _ingest_ts        TIMESTAMP
) USING DELTA PARTITIONED BY (data_evento)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS quarentena_pix (
    payload_raw   STRING,
    motivo        STRING,
    _batch_id     BIGINT,
    _quarentena_ts TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

STATUS_VALIDOS = ["LIQUIDADO", "REJEITADO", "DEVOLVIDO"]

parsed = (
    spark.readStream.table("bronze_pix_eventos")
    .select(
        F.col("payload_raw"),
        F.from_json("payload_raw", schema_pix).alias("e"),
        F.col("_ingest_ts"),
    )
    .select("payload_raw", "e.*", "_ingest_ts")
    .withColumn("data_hora_evento", F.col("data_hora_evento").cast("timestamp"))
    .withColumn("data_evento", F.to_date("data_hora_evento"))
)

# cada regra vira um motivo legível — "dado inválido" não ajuda ninguém às 3h
motivo = (
    F.when(F.col("end_to_end_id").isNull(), "end_to_end_id ausente")
    .when(F.col("conta_pagador").isNull(), "conta_pagador ausente")
    .when(F.col("valor").isNull(), "valor ausente")
    .when(F.col("valor") <= 0, "valor nao positivo")
    .when(~F.col("status").isin(STATUS_VALIDOS), "status desconhecido")
    .when(F.col("data_hora_evento").isNull(), "data_hora_evento invalida")
    .otherwise(F.lit(None))
)

silver_stream = parsed.withColumn("_motivo", motivo)


def processar_batch(batch_df, batch_id):
    # dentro do foreachBatch o DF é batch: dá pra ler duas vezes e usar MERGE
    batch_df.cache()

    ruins = batch_df.filter(F.col("_motivo").isNotNull())
    if not ruins.isEmpty():
        (
            ruins.select(
                "payload_raw",
                F.col("_motivo").alias("motivo"),
                F.lit(batch_id).alias("_batch_id"),
                F.current_timestamp().alias("_quarentena_ts"),
            )
            .write.format("delta").mode("append").saveAsTable("quarentena_pix")
        )

    bons = (
        batch_df.filter(F.col("_motivo").isNull())
        .drop("payload_raw", "_motivo")
        .dropDuplicates(["end_to_end_id"])
    )

    if not bons.isEmpty():
        alvo = DeltaTable.forName(batch_df.sparkSession, "silver_pix_transacoes")
        (
            alvo.alias("t")
            .merge(bons.alias("s"), "t.end_to_end_id = s.end_to_end_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    batch_df.unpersist()


q_silver = (
    silver_stream.writeStream
    .foreachBatch(processar_batch)
    .option("checkpointLocation", f"{CKPT}/silver_pix")
    .trigger(availableNow=True)
    .start()
)

q_silver.awaitTermination()
print("silver:", spark.table("silver_pix_transacoes").count())
print("quarentena:", spark.table("quarentena_pix").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quality gate
# MAGIC
# MAGIC Esta é a célula que transforma o notebook em algo que pode rodar
# MAGIC sozinho. Ela **levanta exceção de propósito** quando o dado está
# MAGIC errado — o que faz o job falhar, aparecer em vermelho no Workflows
# MAGIC e disparar o alerta.
# MAGIC
# MAGIC O ponto que muda a cabeça: em pipeline, job verde não quer dizer
# MAGIC dado certo. Software normal quebra alto — a API cai, a tela dá 500.
# MAGIC Pipeline quebra baixo: roda liso, grava a tabela, e o número está
# MAGIC errado. Quem descobre é o time de negócio, três semanas depois.
# MAGIC Quality gate é você forçando o pipeline a quebrar alto.

# COMMAND ----------

falhas = []

m = spark.sql("""
SELECT
  (SELECT count(*) FROM silver_pix_transacoes)                      AS silver,
  (SELECT count(DISTINCT end_to_end_id) FROM silver_pix_transacoes) AS unicos,
  (SELECT count(*) FROM silver_pix_transacoes WHERE valor <= 0)     AS valor_invalido,
  (SELECT count(*) FROM silver_pix_transacoes WHERE end_to_end_id IS NULL) AS chave_nula,
  (SELECT count(*) FROM quarentena_pix)                             AS quarentena,
  (SELECT count(*) FROM bronze_pix_eventos)                         AS bronze
""").collect()[0]

# unicidade da chave — se quebrar, o MERGE está furado
if m["silver"] != m["unicos"]:
    falhas.append(f"chave duplicada na silver: {m['silver']} linhas, {m['unicos']} únicos")

if m["chave_nula"] > 0:
    falhas.append(f"{m['chave_nula']} linhas com end_to_end_id nulo")

if m["valor_invalido"] > 0:
    falhas.append(f"{m['valor_invalido']} linhas com valor <= 0 passaram pela quarentena")

# volume zero quase sempre é upstream quebrado, não ausência real de PIX
if m["silver"] == 0:
    falhas.append("silver vazia")

# taxa de quarentena — um pico aqui indica mudança de schema na origem
total = m["silver"] + m["quarentena"]
pct = (m["quarentena"] / total * 100) if total else 0.0
if pct > MAX_QUARENTENA_PCT:
    falhas.append(f"quarentena em {pct:.2f}%, acima do limite de {MAX_QUARENTENA_PCT}%")

# reconciliação: tudo que entrou tem que ter ido pra algum lugar
if total != m["bronze"]:
    falhas.append(f"reconciliação: bronze={m['bronze']}, silver+quarentena={total}")

print(f"bronze={m['bronze']} silver={m['silver']} quarentena={m['quarentena']} ({pct:.2f}%)")

if falhas:
    raise Exception("QUALITY GATE FALHOU:\n  - " + "\n  - ".join(falhas))

print("quality gate: OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspeção
# MAGIC
# MAGIC Rode na mão quando quiser olhar o dado. Não faz parte do job.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT motivo, count(*) AS qtd
# MAGIC FROM quarentena_pix GROUP BY motivo ORDER BY qtd DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, count(*) AS qtd, round(sum(valor), 2) AS volume
# MAGIC FROM silver_pix_transacoes GROUP BY status ORDER BY qtd DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### O skew
# MAGIC
# MAGIC As contas PJ aparecem com ordens de grandeza a mais que as PF.
# MAGIC Esse é o cenário pra praticar salting em cima de dado que se
# MAGIC comporta como o de verdade.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT conta_pagador, count(*) AS qtd
# MAGIC FROM silver_pix_transacoes
# MAGIC GROUP BY conta_pagador ORDER BY qtd DESC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios
# MAGIC
# MAGIC 1. **Idempotência** — rode a silver duas vezes sem gerar dado novo.
# MAGIC    A contagem não pode mudar. Depois apague o checkpoint `silver_pix`
# MAGIC    e rode de novo: reprocessa a bronze inteira e a contagem *ainda*
# MAGIC    não muda. Isso é o MERGE fazendo o trabalho dele.
# MAGIC
# MAGIC 2. **Quebrar de propósito** — injete lixo no tópico e veja a
# MAGIC    quarentena pegar e o gate falhar:
# MAGIC
# MAGIC    ```sql
# MAGIC    INSERT INTO pix_topic VALUES
# MAGIC      ('PF-1', '{"end_to_end_id":"X1","valor":-5,"status":"LIQUIDADO"}',
# MAGIC       current_timestamp(), 999999999);
# MAGIC    ```
# MAGIC
# MAGIC 3. **Backlog** — rode o gerador 3x sem rodar este notebook, depois
# MAGIC    processe tudo de uma vez. É o cenário de plantão pós-incidente.
# MAGIC
# MAGIC 4. **Evolução de schema** — adicione um campo no gerador. A bronze
# MAGIC    não liga (é crua), a silver ignora até você mexer no `schema_pix`.
# MAGIC    Esse é o motivo da bronze existir.

# COMMAND ----------

for q in spark.streams.active:
    print("parando:", q.name or q.id)
    q.stop()
