# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df_internacao = spark.table("classes.gold.df_internacao")
df_internacao.show()

# COMMAND ----------

#Valor total por procedimento
df_valor_total = df_gold.groupBy(
    "procedimento_id",
    "procedimento_descricao"
).agg(
    F.sum("valor_aprovado").alias("valor_total_aprovado")
)

# COMMAND ----------

#Quantidade total por procedimento
df_quantidade_total = df_gold.groupBy(
    "procedimento_id",
    "procedimento_descricao"
).agg(
    F.sum("quantidade_aprovada").alias("quantidade_total_aprovada")
)

# COMMAND ----------


#Top 10 procedimentos mais caros
df_top10_caros = df_valor_total.orderBy(
    F.desc("valor_total_aprovado")
).limit(10)

df_top10_caros.show(truncate=False)

# COMMAND ----------

#Top 10 procedimentos mais aprovados
df_top10_mais_aprovados = df_quantidade_total.orderBy(
    F.desc("quantidade_total_aprovada")
).limit(10)

df_top10_mais_aprovados.show(truncate=False)

# COMMAND ----------

#Top 10 procedimentos menos aprovados
df_top10_menos_aprovados = df_quantidade_total.orderBy(
    F.asc("quantidade_total_aprovada")
).limit(10)

df_top10_menos_aprovados.show(truncate=False)

# COMMAND ----------

#total de aprovações
from pyspark.sql import functions as F

total_aprovados = df_internacao.agg(
    F.sum("quantidade_aprovada").alias("total_aprovados")
)

total_aprovados.show()

# COMMAND ----------

#total de valor
from pyspark.sql import functions as F

total_valor = df_internacao.agg(
    F.sum("valor_aprovado").alias("total_valor")
)

total_valor.show(truncate=False)
