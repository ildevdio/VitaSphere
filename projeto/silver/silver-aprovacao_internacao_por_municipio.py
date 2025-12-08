# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, lit, when, trim, regexp_replace,
    datediff, months_between
)
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, DateType
)

# COMMAND ----------

df_aprovacao = spark.sql("select * from classes.bronze.aprovacao_internacao_por_municipio")

# COMMAND ----------

#muda os nomes das colunas
df_aprovacao = (
    df_aprovacao
        .withColumnRenamed('"""Munic�pio""', 'municipio_desc')
        .withColumnRenamed('""Quantidade aprovada""', 'quantidade_aprovada')
        .withColumnRenamed('""Valor aprovado"""', 'valor_aprovado')
)

# COMMAND ----------

# troca vírgula por ponto
df_aprovacao = df_aprovacao.withColumn(
    "valor_aprovado",
    regexp_replace(col("valor_aprovado"), ",", ".")
)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

#remove todos os caracteres especiais mantendo letras, numeros, ponto e virgula
for c, t in df_aprovacao.dtypes:
    if t == "string":
        df_aprovacao = df_aprovacao.withColumn(
            c,
            regexp_replace(col(c), r"[^0-9A-Za-zÀ-ÿ .,]", "")
        )

# COMMAND ----------

from pyspark.sql.functions import when, col, trim, expr
#se for vazio vira 0
df_aprovacao = df_aprovacao.withColumn(
    "valor_aprovado",
    expr("try_cast(valor_aprovado AS double)")
)

df_aprovacao = df_aprovacao.withColumn(
    "quantidade_aprovada",
    expr("try_cast(quantidade_aprovada AS bigint)")
)

# COMMAND ----------

# remove tudo que não é número e .
df_aprovacao = df_aprovacao.withColumn(
    "valor_aprovado",
    regexp_replace(col("valor_aprovado"), r"[^0-9.]", "") 
)

# COMMAND ----------

from pyspark.sql.functions import round, col

#transforma coluna de valor_aprovado em double
df_aprovacao = df_aprovacao.withColumn(
    "valor_aprovado",
    round(col("valor_aprovado"), 2)
)

# COMMAND ----------

#transforma coluna de quantidade_aprovada em bigint
df_aprovacao = df_aprovacao.withColumn(
    "quantidade_aprovada",
    regexp_replace(col("quantidade_aprovada"), r"[^0-9]", "").cast("bigint")
)

# COMMAND ----------

#extrai id e descricao do procedimento
from pyspark.sql.functions import regexp_extract

df_aprovacao = df_aprovacao.withColumn("municipio_id", regexp_extract("municipio_desc", r"^(\d+)", 1)) \
       .withColumn("municipio", regexp_extract("municipio_desc", r"^\d+\s*(.*)$", 1))
df_aprovacao = df_aprovacao.drop("municipio_desc")

# COMMAND ----------

#muda a ordem de exibição das colunas
df_aprovacao = df_aprovacao.select("municipio_id", "municipio", "quantidade_aprovada", "valor_aprovado")

# COMMAND ----------

#tira caracteres especiais de municipio
from pyspark.sql.functions import regexp_replace, col

df_aprovacao = df_aprovacao.withColumn(
    "municipio",
    regexp_replace(col("municipio"), r"[^\x20-\x7EÀ-ÿ]", "")
)

# COMMAND ----------

display(df_aprovacao)

# COMMAND ----------

df_aprovacao.write.saveAsTable(
    "classes.gold.df_aprovacao_internacao_por_municipio",
    mode="overwrite"
)
